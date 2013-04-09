using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class ProposerLeader
    {
        private readonly object _syncLock;
        private readonly int _majority;
        private readonly ProposerRecord[] _records;
        private ClientValueWrapper _clientListHead;
        private ClientValueWrapper _clientListTail;
        private int _currentInstanceId;
        private int _leaderId;

        // Phase 1 info
        private PendingPromiseMessage _phase1PendingListHead;
        private PendingPromiseMessage _phase1PendingListTail;
        private int _phase1HighestOpen;
        private int _phase1ReadyCount;
        private int _phase1PendingCount;
        
        // Phase 2 info
        private int _phase2NextUnusedInstanceId;
        private int _phase2OpenCount;

        // Infinite prepare info
        private int	_infinitePrepareOpened;
        private int _infinitePreparePromisesBitVector;
        private int _infinitePreparePromisesCount;
        private StatusFlag _infinitePrepareStatus;
        private int _infinitePrepareMyBallot;

        private ClientValueWrapper _phase2PendingAccept;
        private DateTime _phase2PendingTimer;
        private IDisposable _messageBusSubscription;

        public ProposerLeader(PaxosConfiguration configuration, Learner learner, int id)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }

            if (learner == null)
            {
                throw new ArgumentNullException("learner");
            }

            if (id < 0 || id >= configuration.ProposerCountMaxValue)
            {
                throw new ArgumentOutOfRangeException("id");
            }

            Configuration = configuration;
            Id = id;
            Learner = learner;
            Status = ProcessStatus.Stopped;

            _leaderId = 0;
            _syncLock = new object();
            _phase1ReadyCount = 0;
            _phase1PendingCount = 0;
            _phase1PendingListHead = null;
            _phase1PendingListTail = null;
            _phase2PendingAccept = null;
            _clientListHead = null;
            _clientListTail = null;
            _phase1HighestOpen = -1;
            _currentInstanceId = 0;
            _majority = (Configuration.AcceptorCount / 2) + 1;
            _records = new ProposerRecord[configuration.ProposerRecordCapacity];
            for (var i = 0; i < _records.Length; i++)
            {
                _records[i] = new ProposerRecord(Configuration.AcceptorCount);
            }

            Log("P{0}: Init completed", Id);
        }

        internal enum StatusFlag
        {
            Empty,
            Phase1Pending,
            Phase1Ready,
            Phase2Reserved, // TODO : REMOVE
            Phase2Pending,
            Phase2Completed
        }

        public int Id { get; private set; }

        public Learner Learner { get; private set; }

        public PaxosConfiguration Configuration { get; private set; }

        public ProcessStatus Status { get; private set; }

        public bool IsLeader 
        {
            get
            {
                return Id == _leaderId;
            }
        }

        public void Start()
        {
            if (Status != ProcessStatus.Stopped)
            {
                return;
            }

            Status = ProcessStatus.Starting;

            if (IsLeader)
            {
                InitialiseAsLeader();
            }

            Task.Factory.StartNew(TimeoutChecksLoop, TaskCreationOptions.LongRunning);
            Task.Factory.StartNew(DeliveryCheckLoop, TaskCreationOptions.LongRunning);
            _messageBusSubscription = Configuration.MessageBus.Subscribe(OnRecieveMessage);
            Status = ProcessStatus.Running;
        }
        
        public void Stop()
        {
            Status = ProcessStatus.Stopping;
            _messageBusSubscription.Dispose();
        }

        public void SubmitValue(string value)
        {
            lock (_syncLock)
            {
                NoLockProposerSubmitValue(value);
                PeriodicPhase2Check();
            }
        }

        private static void SetLeaderExpiration(ProposerRecord record, TimeSpan interval)
        {
            record.Timeout = DateTime.Now + interval;
        }

        private static bool IsLeaderExpired(DateTime deadline, DateTime now)
        {
            return now > deadline;
        }

        private static void LeaderSetNextPhase1Check()
        {
            // int ret = event_add(&p1_check_event, &p1_check_interval);
            // assert(ret == 0);
        }

        private static void LeaderSetNextPhase2Check()
        {
            // Phase 2 routines
            // int ret = event_add(&p2_check_event, &p2_check_interval);
            // assert(ret == 0);
        }

        private static void Log(int x, string format, params object[] args)
        {

        }

        private static void ClearInstanceInfo(ProposerRecord record)
        {
            record.InstanceId = 0;
            record.Status = StatusFlag.Empty;
            record.BallotId = 0;
            record.Phase1ValueBallotId = 0;
            record.PromiseCount = 0;
            record.Phase1Value = null;
            record.Phase2Value = null;
            for (var i = 0; i < record.Promises.Length; i++)
            {
                record.Promises[i] = null;
            }
        }

        private void NoLockProposerSubmitValue(string value)
        {
            Log("P{0}: Submitting value of size {1} to proposer", Id, value.Length);
            AddToClientValueListTail(new ClientValueWrapper { Value = value });
            PeriodicPhase2Check();
        }

        private void Log(string format, params object[] args)
        {
            Configuration.TraceSource.TraceEvent(TraceEventType.Verbose, 0, format, args);
        }

        private void AddToClientValueListTail(ClientValueWrapper wrapper)
        {
            if (_clientListHead == null)
            {
                _clientListHead = wrapper;
                _clientListTail = wrapper;
            }
            else
            {
                _clientListTail.Next = wrapper;
                _clientListTail = wrapper;
            }
        }

        private Task<bool> OnRecieveMessage(Message message)
        {
            if (message is PrepareResponseMessage)
            {
                OnRecievePromiseMessage(message as PrepareResponseMessage);
                return Task<bool>.Factory.StartNew(() => true);
            }

            return Task<bool>.Factory.StartNew(() => false);
        }

        private void OnRecievePromiseMessage(PrepareResponseMessage promiseMessage)
        {
            lock (_syncLock)
            {
                var record = _records[GetRecordIndex(promiseMessage.InstanceId)];
                if (HandlePromiseMessage(promiseMessage, record))
                {
                    // Majority reached
                    CheckReady(record);
                }

                PeriodicPhase2Check();
            }
        }

        private void PrepareNewInstances()
        {
            // Proposer tries to open instances from... to...
            var lastToOpen = (Configuration.ProposerPreExecWinSize - (_phase1ReadyCount + _phase1PendingCount)) + _phase1HighestOpen;

            Log("P{0}: Proposer tries to open instances from {1} to {2}", Id, _phase1HighestOpen + 1, lastToOpen);
            for (var i = _phase1HighestOpen + 1; i <= lastToOpen; i++)
            {
                // Add prepare to send_buffer
                Configuration.MessageBus.Publish(new PrepareRequestMessage(i, GetNewBallotId()));
                
                // Add to proposer_array
                var record = _records[GetRecordIndex(i)];
                
                // Clears old promises and set status to p1_pending
                InitializeProposerRecord(record, i, GetNewBallotId());
            }
       
            AddToP1PendingList(_phase1HighestOpen + 1, lastToOpen);
            _phase1PendingCount += lastToOpen - _phase1HighestOpen;
            _phase1HighestOpen = lastToOpen;
        }

        private int GetNewBallotId()
        {
            return Configuration.ProposerCountMaxValue + Id;
        }

        private int GetNextBallotId(int ballotId)
        {
            return ballotId + Configuration.ProposerCountMaxValue;
        }

        private void PeriodicPhase2Check()
        {
            // Phase 2
            var record = _records[GetRecordIndex(_currentInstanceId)];
            if (record.InstanceId == _currentInstanceId)
            {
                if (record.Status == StatusFlag.Phase1Ready)
                {
                    DeliverNextValue(record);
                }
                else if (record.Status == StatusFlag.Phase2Reserved)
                {
                    // Phase 2 some elses value            
                    DeliverReservedValue(record);
                }
            }

            // Phase 1 pre-execution 
            if (_phase1ReadyCount + _phase1PendingCount < (Configuration.ProposerPreExecWinSize / 2))
            {
                PrepareNewInstances();
            }
        }

        private void DeliverReservedValue(ProposerRecord record)
        {
            var message = record.ReservedPromise;
            if (RemoveFromPendingIfMatches(message.Value)) 
            {
                Log("P{0}: Value was pushed back in pending list, removing it", Id);
            }

            Log("P{0}: Delivering reserved value for instance {1}", Id, record.InstanceId);
            var wrapper = new ClientValueWrapper { Value = message.Value };
            SendAcceptMessage(record, wrapper);
            record.Status = StatusFlag.Phase2Pending;
            _phase2PendingAccept = wrapper;
            _phase2PendingTimer = DateTime.Now;
        }

        private void SendAcceptMessage(ProposerRecord record, ClientValueWrapper clientValueWrapper)
        {
            Configuration.MessageBus.Publish(new AcceptRequestMessage(record.InstanceId, record.BallotId, clientValueWrapper.Value));
        }

        private void DeliverNextValue(ProposerRecord record)
        {
            ClientValueWrapper valueWrapper = null;
            if (_clientListHead != null)
            {
                valueWrapper = _clientListHead;
                _clientListHead = valueWrapper.Next;
                if (_clientListTail == valueWrapper)
                {
                    _clientListTail = null;
                }
            }

            if (valueWrapper == null)
            {
                return;
            }

            Log("P{0}: Delivering next value", Id);
            SendAcceptMessage(record, valueWrapper);
            record.Status = StatusFlag.Phase2Pending;
            _phase2PendingAccept = valueWrapper;
            _phase2PendingTimer = DateTime.Now;
            _phase1ReadyCount--;
        }

        private void CheckReady(ProposerRecord record)
        {
            var mostRecentBallot = -1;
            var mostRecentBallotIndex = -1;

            for (var i = 0; i < Configuration.AcceptorCount; i++) 
            {
                var promiseMessage = record.Promises[i];
                if (promiseMessage == null)
                {
                    continue;
                }

                if (promiseMessage.Value == null)
                {
                    continue;
                }

                // Value ballot is -1 if there is no value
                if (promiseMessage.ValueBallotId > mostRecentBallot) 
                {
                    mostRecentBallot = promiseMessage.ValueBallotId;
                    mostRecentBallotIndex = i;
                }   
            }
    
            // No value found
            if (mostRecentBallotIndex == -1) 
            {
                record.Status = StatusFlag.Phase1Ready;
                _phase1ReadyCount++;
                _phase1PendingCount--;

                Log(
                    "P{0}: Instance {1} is now ready! (pen:{2} read:{3})",
                    Id,
                    record.InstanceId,
                    _phase1PendingCount,
                    _phase1ReadyCount);
                return;
            }
    
            // Value was found, send an accept
            record.Status = StatusFlag.Phase2Reserved;
            record.ReservedPromise = record.Promises[mostRecentBallotIndex];

            Log("P{0}: Instance {1} is now reserved!", Id, record.InstanceId);
            _phase1PendingCount--;
        }

        private bool HandlePromiseMessage(PrepareResponseMessage message, ProposerRecord record)
        {
            // Ignore because there is no info about
            // this iid in proposer_array
            if (record.InstanceId != message.InstanceId) 
            {
                Log("P{0}: Promise for {1} ignored, no info in array", Id, message.InstanceId);
                return false;
            }
    
            // This promise is not relevant, because
            // we are not waiting for promises for instance iid
            if (record.Status != StatusFlag.Phase1Pending) 
            {
                Log("P{0}: Promise for {1} ignored, instance is not p1_pending", Id, message.InstanceId);
                return false;
            }
    
            // This promise is old, or belongs to another
            if (record.BallotId != message.BallotId) 
            {
                Log("P{0}: Promise for {1} ignored, not our ballot", Id, message.InstanceId);
                return false;
            }
    
            // Already received this promise
            if (record.Promises[message.AcceptorId] != null) 
            {
                Log("P{0}: Promise for {1} ignored, already received", Id, message.InstanceId);
                return false;
            }
    
            // Received promise, save it in proposer_array
            var copy = new PrepareResponseMessage(
                message.InstanceId, message.BallotId, message.ValueBallotId, message.Value, message.AcceptorId);
            record.Promises[message.AcceptorId] = copy;
            record.PromiseCount++;
    
            if (record.PromiseCount < _majority)
            {
                Log("P{0}: Promise for {1} received, not a majority yet", Id, message.InstanceId);
                return false;
            }

            Log("P{0}: Promise for {1} received, majority reached!", Id, message.InstanceId);
            return true;
        }

        private void DeliveryCheckLoop()
        {
            while (Status == ProcessStatus.Starting || Status == ProcessStatus.Running)
            {
                var learnerMessage = Learner.GetNextValueWrapper();
                lock (_syncLock)
                {
                    VerifyAcceptedValue(learnerMessage);
                    PeriodicPhase2Check();
                }

                // if (deliver_callback != null)
                // {
                //    deliver_callback(lmsg.value, lmsg.value_size);
                // } 
            }

            Status = ProcessStatus.Stopped;
        }

        private void VerifyAcceptedValue(Learner.LearnerValueWrapper learnerMessage)
        {
            // Check if accepted value is the one we sent
            // If not, re-enqueue the value
            if (learnerMessage.InstanceId != _currentInstanceId) 
            {
                // Learn message contains InstanceId... while CurrentInstanceId is...
                Environment.Exit(0);
            }

            _currentInstanceId++;
    
            // Check if value is first in value list
            // (can happen if p2 expires and restarts from p1 while value is actually delivered)
            if (RemoveFromPendingIfMatches(learnerMessage.Value))
            {
                Log("P{0}: Value accepted, instance {1} removing from client list head", Id, _currentInstanceId - 1);
                return;
            }
    
            // We were not waiting for a learn message
            if (_phase2PendingAccept == null)
            {
                Log("P{0}: Someone's value was learned for instance {1}", Id, _currentInstanceId - 1);
                return;
            }

            if (learnerMessage.Value != _phase2PendingAccept.Value) 
            {
                // Another value was accepted
                Log("P{0}: A different value was learned for instance {1}, re-queuing our value", Id, _currentInstanceId - 1);
                ReSubmitPendingAccept();
            } 
            else
            {
                // Our value was accepted
                Log("P{0}: Value accepted, instance {1}", Id, _currentInstanceId - 1);
                _phase2PendingAccept = null;
            }
        }
        
        private bool RemoveFromPendingIfMatches(string value)
        {
            var cvw = _clientListHead;
            if ((cvw != null) && (cvw.Value == value))
            {
                // Remove first value
                _clientListHead = _clientListHead.Next;
                if (_clientListTail == cvw)
                {
                    // Was the only item
                    _clientListTail = null;
                }

                return true;
            }

            return false;
        }
        
        private void TimeoutChecksLoop()
        {
            var timeoutCheckInterval = GetTimeoutCheckInterval();
            
            lock (_syncLock)
            {
                while (Status == ProcessStatus.Starting || Status == ProcessStatus.Running)
                {
                    Monitor.Wait(_syncLock, timeoutCheckInterval);

                    // Phase 1 timeout check
                    if (_phase1PendingCount > 0)
                    {
                        CheckP1Pending();
                    }

                    // Phase 2 timeout check
                    if (_phase2PendingAccept != null)
                    {
                        CheckP2Pending();
                    }

                    // Phase 1 PreExecution
                    if (_phase1ReadyCount + _phase1PendingCount < (Configuration.ProposerPreExecWinSize / 2))
                    {
                        PrepareNewInstances();
                    }
                }

                Status = ProcessStatus.Stopped;
            }
        }

        private TimeSpan GetTimeoutCheckInterval()
        {
            if (Configuration.PromiseTimeout < Configuration.AcceptTimeout)
            {
                return TimeSpan.FromMilliseconds(Configuration.PromiseTimeout.TotalMilliseconds / 2);
            }

            return TimeSpan.FromMilliseconds(Configuration.AcceptTimeout.TotalMilliseconds / 2);
        }
        
        private void CheckP2Pending()
        {
            if (_phase2PendingTimer + Configuration.AcceptTimeout > DateTime.Now)
            {
                Log("P{0}: Accept timeout for instance {1} is not expired yet", Id, _currentInstanceId);
                return;
            }

            Log("P{0}:  ! Accept for instance {0} timed out, restarts from phase1 !", Id, _currentInstanceId);
                
            // Re-queue value in client list
            ReSubmitPendingAccept();

            // Re-execute phase 1 with higher ballot
            var record = _records[GetRecordIndex(_currentInstanceId)];

            // Clears old promises and set status to p1_pending
            InitializeProposerRecord(record, _currentInstanceId, BallotNext(record.BallotId));

            // Publish prepare message
            Configuration.MessageBus.Publish(new PrepareRequestMessage(_currentInstanceId, record.BallotId));

            AddToP1PendingList(_currentInstanceId, _currentInstanceId);
            _phase1PendingCount++;
            _phase2PendingAccept = null;
        }

        private void AddToP1PendingList(int fromInstanceId, int toInstanceId)
        {
            var message = new PendingPromiseMessage
                {
                    FromInstanceId = fromInstanceId,
                    ToInstanceId = toInstanceId,
                    Timer = DateTime.Now,
                    Next = null
                };

            if (_phase1PendingListHead == null)
            {
                // List is empty
                _phase1PendingListHead = message;
                _phase1PendingListTail = message;
            }
            else
            {
                // List is not empty, update next of last tail
                _phase1PendingListTail.Next = message;
                _phase1PendingListTail = message;
            }

            Log("P{0}: Added P1 timeout from {1} to {2}", Id, fromInstanceId, toInstanceId);
        }

        private void InitializeProposerRecord(ProposerRecord record, int instanceId, int ballotId)
        {
            record.InstanceId = instanceId;
            record.BallotId = ballotId;
            record.Status = StatusFlag.Phase1Pending;
            record.PromiseCount = 0;
            record.ReservedPromise = null;
            for (var i = 0; i < record.Promises.Length; i++)
            {
                record.Promises[i] = null;
            }
        }

        private int BallotNext(int lastBallotId)
        {
            return lastBallotId + Configuration.ProposerCountMaxValue;
        }

        private int GetRecordIndex(int instanceId)
        {
            return instanceId & (_records.Length - 1);
        }

        private void ReSubmitPendingAccept()
        {
            AddToClientValueListHead(_phase2PendingAccept);
        }

        private void AddToClientValueListHead(ClientValueWrapper wrapper)
        {
            if (_clientListHead == null)
            {
                _clientListHead = wrapper;
                _clientListTail = wrapper;
            }
            else
            {
                wrapper.Next = _clientListHead;
                _clientListHead = wrapper;
            }
        }

        private void CheckP1Pending()
        {
            PendingPromiseMessage message;
            while ((message = PopNextP1Timeout()) != null)
            {
                var now = DateTime.Now;
                if (message.Timer + Configuration.PromiseTimeout > now)
                {
                    Log("P{0}: Timer for next p1_pending is not yet expired", Id);
                    break;
                }

                // Next timeout is expired
                CheckExpiredP1Range(message.FromInstanceId, message.ToInstanceId);
            }

            if (message != null)
            {
                // Next item not expired
                // Requeue as first
                PushNextP1Timeout(message);
            }
        }

        private void PushNextP1Timeout(PendingPromiseMessage message)
        {
            if (_phase1PendingListHead == null && _phase1PendingListTail == null)
            {
                _phase1PendingListHead = message;
                _phase1PendingListTail = message;
            }
            else
            {
                message.Next = _phase1PendingListHead;
                _phase1PendingListHead = message;
            }
        }

        private void CheckExpiredP1Range(int fromInstanceId, int toInstanceId)
        {
            var lowerReopen = -1;
            var higherReopen = -1;
            
            for (var i = fromInstanceId; i <= toInstanceId; i++)
            {
                var record = _records[GetRecordIndex(i)];
                if (record.InstanceId != i || record.Status != StatusFlag.Phase1Pending)
                {
                    // Skip this one
                    Log("P{0}: iid:{1} is not p1_pending, skipped", Id, record.InstanceId);
                    continue;
                }

                if (i < _currentInstanceId)
                {
                    // Skip this one
                    Log("P{0}: iid:{1} is already closed, skipped", Id, i);
                    continue;
                }
                   
                // Increment ballot
                InitializeProposerRecord(record, i, BallotNext(record.BallotId));

                // Add to send_buffer
                // Raised ballot for prepare of InstanceId
                Log("P{0}: Raised ballot to {1} for prepare of instance {2}", Id, record.BallotId, record.InstanceId);
                Configuration.MessageBus.Publish(new PrepareRequestMessage(i, record.BallotId));

                // Keep track of min and max expired
                if (lowerReopen == -1)
                {
                    lowerReopen = i;
                }

                higherReopen = i;
            }
        
            // Set timer for newly reopened
            if (lowerReopen != -1)
            {
                AddToP1PendingList(lowerReopen, higherReopen);
            }
        }

        private PendingPromiseMessage PopNextP1Timeout()
        {
            PendingPromiseMessage p;
            if (_phase1PendingListHead == null)
            {
                // List is empty
                p = null;
            }
            else if (_phase1PendingListHead == _phase1PendingListTail)
            {
                // Only 1 element in list
                p = _phase1PendingListHead;
                _phase1PendingListHead = null;
                _phase1PendingListTail = null;
            }
            else
            {
                // List has 2 or more elements, set a new first
                p = _phase1PendingListHead;
                _phase1PendingListHead = _phase1PendingListHead.Next;
            }

            return p;    
        }
        
        private void LeaderCheckPhase1Pending() 
        {
            // Create an empty prepare batch in send buffer
            // sendbuf_clear(to_acceptors, prepare_reqs, this_proposer_id);
            Log(1, "Checking pending phase 1 from {0} to {1}", _currentInstanceId, _phase1HighestOpen);

            // Get current time for checking expired    
            var now = DateTime.Now;
            for (var id = _currentInstanceId; id <= _phase1HighestOpen; id++) 
            {
                // Get instance from state array
                var record = _records[GetRecordIndex(id)];
                
                // assert(record.iid == iid_iterator);
                // Still pending . it's expired
                if (record.Status == StatusFlag.Phase1Pending && IsLeaderExpired(record.Timeout, now)) 
                {
                    Log(1, "Phase 1 of instance {0} expired!", record.InstanceId);

                    // Reset fields used for previous phase 1
                    // record.promises_bitvector = 0;
                    record.PromiseCount = 0;
                    record.Phase1ValueBallotId = 0;
                    record.Phase1Value = null;
                    
                    // Ballot is incremented
                    record.BallotId = GetNextBallotId(record.BallotId);
                    
                    // Send prepare to acceptors
                    // sendbuf_add_prepare_req(to_acceptors, record.iid, record.my_ballot);        
                    SetLeaderExpiration(record, Configuration.Phase1Timeout);
                    
                    // COUNT_EVENT(p1_timeout);
                }
            }    

            // Send if something is still there
            // sendbuf_flush(to_acceptors);
        }

        private void LeaderOpenInstancesPhase1() 
        {
            // Opens instances at the "end" of the proposer state array 
            // Those instances were not opened before

            // var active_count = _phase1PendingCount + _phase1ReadyCount;
            // assert(active_count >= 0);
            if (_infinitePrepareStatus == StatusFlag.Empty) 
            {
                // Ask for infinite p1 instances
                // Create an empty prepare batch in send buffer
                // sendbuf_clear(to_acceptors, prepare_reqs, this_proposer_id);

                // Send prepare to acceptors
                // sendbuf_add_prepare_req(to_acceptors, INF_PREPARE_REQ, FIRST_BALLOT);

                // Send if something is still there
                // sendbuf_flush(to_acceptors);

                // Keep track of pending count
                _infinitePrepareStatus = StatusFlag.Phase1Pending;
                _infinitePrepareMyBallot = GetNewBallotId();
                Log(1, "Opened {0} new instances", 1);
            }
        }

        private void LeaderPeriodicPhase1Check()
        {
            // This function is invoked periodically
            // (periodic_repeat_interval) to retrasmit the most 
            // recent accept
        
            // All instances in status p1_pending are expired
            // increment ballot and re-send prepare_req
            LeaderCheckPhase1Pending();

            // Try to open new instances if some were used
            LeaderOpenInstancesPhase1();

            // Set next check timeout for calling this function
            LeaderSetNextPhase1Check();
        }

        private void LeaderExecutePhase2(ProposerRecord ii) 
        {
            if (ii.Phase1Value == null && ii.Phase2Value == null)
            {
                // Happens when p1 completes without value        
                // Assign a Phase2Value and execute
                ii.Phase2Value = vh_get_next_pending();
                
                // assert(record.Phase2Value != null);

            }
            else if (ii.Phase1Value != null)
            {
                // Only p1 value is present, MUST execute p2 with it
                // Save it as p2 value and execute
                ii.Phase2Value = ii.Phase1Value;
                ii.Phase1Value = null;
                ii.Phase1ValueBallotId = 0;

            }
            else if (ii.Phase2Value != null)
            {
                // Only p2 valye is present
                // Do phase 2 with it
            }
            else
            {
                // There are both p1 and p2 value
                // Compare them
                if (vh_value_compare(ii.Phase1Value, ii.Phase2Value) == 0)
                {
                    // Same value, just delete Phase1Value
                    ii.Phase1Value = null;
                    ii.Phase1ValueBallotId = 0;
                }
                else
                {
                    // Different values
                    // Phase2Value is pushed back to pending list
                    vh_push_back_value(ii.Phase2Value);
                    
                    // Must execute p2 with p1 value
                    ii.Phase2Value = ii.Phase1Value;
                    ii.Phase1Value = null;
                    ii.Phase1ValueBallotId = 0;
                }
            }
            
            // Change instance status
            ii.Status = StatusFlag.Phase2Pending;

            // Send the accept request
            // sendbuf_add_accept_req(to_acceptors, record.iid, record.my_ballot, record.Phase2Value.value, record.Phase2Value.value_size);

            // Set the deadline for this instance
            SetLeaderExpiration(ii, Configuration.Phase2Timeout);
        }
        
        private void LeaderOpenInstancesPhase2Expired() 
        {
            // Scan trough p1_ready that have a value
            // assigned or found, execute phase2
            int count = 0;

            // Create a batch of accept requests
            // sendbuf_clear(to_acceptors, accept_reqs, this_proposer_id);

            // Start new phase 2 for all instances found in status p1_ready
            // if they are in the range below, phase2 timed-out and 
            // we went successfully trough phase1 again
            for (var id = _currentInstanceId; id < _phase2NextUnusedInstanceId; id++) 
            {
                var record = _records[GetRecordIndex(id)];
                
                // assert(record.iid == iid);
                // This instance is in status p1_ready but it's in the range
                // of previously opened phase2, it's now time to retry
                if (record.Status == StatusFlag.Phase1Ready) 
                {
                    // assert(record.Phase2Value != null || record.Phase2Value != null);
                    LeaderExecutePhase2(record);
                    
                    // Count opened
                    count += 1;
                }
            }

            // Count p1_ready that were consumed
            _phase1ReadyCount -= count;

            // Flush last accept_req batch
            // sendbuf_flush(to_acceptors);
            Log(1, "Opened {0} old (timed-out) instances", count);
        }
        
        private void LeaderOpenInstancesPhase2New() 
        {
            // For better batching, opening new instances at the end
            // is preferred when more than 1 can be opened together
            var threshold = (Configuration.ProposerPhase2Concurrency / 3 ) * 2;
            if (_phase2OpenCount > threshold) 
            {
                Log(1, "Skipping Phase2 open, {0} are still active (tresh:{1})", _phase2OpenCount, threshold);
                return;
            }

            Log(1, "Could open {0} p2 instances", Configuration.ProposerPhase2Concurrency - _phase2OpenCount);

            // Create a batch of accept requests
            // sendbuf_clear(to_acceptors, accept_reqs, this_proposer_id);

            // Start new phase 2 while there is some value from 
            // client to send and we can open more concurrent instances
            var count = 0;
            while ((count + _phase2OpenCount) <= Configuration.ProposerPhase2Concurrency) 
            {
                var record = _records[GetRecordIndex(_phase2NextUnusedInstanceId)];
                
                // assert(record.Phase2Value == null);
                // No value to send for next unused, stop
                if (record.Phase1Value == null && vh_pending_list_size() == 0) 
                {
                    Log(1, "No value to use for next instance");
                    break;
                }

                var normalReady = record.Status == StatusFlag.Phase1Ready && record.InstanceId != _phase2NextUnusedInstanceId;
                var infReady = record.Status == StatusFlag.Empty && _infinitePrepareStatus == StatusFlag.Phase1Ready;
                var ready = normalReady | infReady;
                
                // assert(record.Phase2Value == null || inf_ready);
                // Next unused is not ready, stop
                if (!ready) 
                {
                    Log(1, "Next instance to use for P2 (iid:{0}) is not ready yet", _phase2NextUnusedInstanceId);
                    Log(1, "{0} {1} {2} {3}", record.Status, _infinitePrepareStatus, record.InstanceId, _phase2NextUnusedInstanceId);
                    
                    // COUNT_EVENT(p2_waits_p1);
                    break;
                }

                if (infReady) 
                {
                    record.InstanceId = _phase2NextUnusedInstanceId;
                }

                // Executes phase2, sending an accept request
                // Using the found value or getting the next from list
                LeaderExecutePhase2(record);

                // Count opened
                count += 1;
                
                // Update next to use
                _phase2NextUnusedInstanceId += 1;
            }

            // Count newly opened
            _phase2OpenCount += count;
            
            // Flush last accept_req batch
            // sendbuf_flush(to_acceptors);
            if (count > 0) 
            {
                Log(1, "Opened {0} new instances", count);
            }
        }

        private void LeaderPeriodicPhase2Check()
        {
            // create a prepare batch for expired instances
            // sendbuf_clear(to_acceptors, prepare_reqs, this_proposer_id);
            var now = DateTime.Now;

            // from current to highest open, check deadline
            // if instances in status p2_pending
            for (var id = _currentInstanceId; id < _phase2NextUnusedInstanceId; id++) 
            {
                var record = _records[GetRecordIndex(id)];

                // Not p2_pending, skip
                if (record.Status != StatusFlag.Phase2Pending)
                {
                    continue;
                }

                // Check if it was closed in the meanwhile
                // (but not delivered yet)
                if (Learner.IsClosed(id)) 
                {
                    record.Status = StatusFlag.Phase2Completed;
                    _phase2OpenCount -= 1;
                    
                    // The rest (i.e. answering client)
                    // is done when the value is actually delivered
                    Log(1, "Instance {0} closed, waiting for deliver", id);
                    continue;
                }

                // Not expired yet, skip
                if(!IsLeaderExpired(record.Timeout, now)) 
                {
                    continue;
                }

                // Expired and not closed: must restart from phase 1
                record.Status = StatusFlag.Phase1Pending;
                _phase1PendingCount += 1;
                record.BallotId = GetNextBallotId(record.BallotId);
                
                // Send prepare to acceptors
                // sendbuf_add_prepare_req(to_acceptors, record.iid, record.my_ballot);
                SetLeaderExpiration(record, Configuration.Phase1Timeout);
                Log(1, "Instance {0} restarts from phase 1", id);

                // COUNT_EVENT(p2_timeout);
            }

            // Flush last message if any
            // sendbuf_flush(to_acceptors);

            // Open new instances
            LeaderOpenInstancesPhase2New();

            // Set next invokation of this function
            LeaderSetNextPhase2Check();
    }
        
        private void LeaderDeliver(string value, int size, int iid)
        {
            Log(1, "Instance {0} delivered to Leader", iid);

            // Verify that the value is the one found or associated
            var record = _records[GetRecordIndex(iid)];

            // Instance not even initialized, skip
            if (record.InstanceId != iid) 
            {
                return;
            }

            if (record.Status == StatusFlag.Phase1Ready)
            {
                _phase1PendingCount -= 1;
            }

            if (_phase2NextUnusedInstanceId == iid)
            {
                _phase2NextUnusedInstanceId += 1;
            }

            var openedByMe = (record.Status == StatusFlag.Phase1Pending && record.Phase2Value != null)
                               || (record.Status == StatusFlag.Phase1Ready && record.Phase2Value != null)
                               || (record.Status == StatusFlag.Phase2Pending);

            if (openedByMe) 
            {
                _phase2OpenCount -= 1;
            }

            var myVal = (record.Phase2Value != null) && (value == record.Phase2Value.Value);
            if (myVal) 
            {
                // Our value accepted, notify client that submitted it
                vh_notify_client(0, record.Phase2Value);
            }
            else if (record.Phase2Value != null) 
            {
                // Different value accepted, push back our value
                vh_push_back_value(record.Phase2Value);
                record.Phase2Value = null;
            }
            else 
            {
                // We assigned no value to this instance, 
                // it comes from somebody else??
            }

            // Clear current instance
            ClearInstanceInfo(record);

            // If enough instances are ready to 
            // be opened, start phase2 for them
            LeaderOpenInstancesPhase2New();
        }

        private void InitialiseAsLeader() 
        {
            Log(0, "Proposer %d promoted to leader\n", Id);

            // #ifdef LEADER_EVENTS_UPDATE_INTERVAL
            //    ClearEventCounters();
            //    evtimer_set(&print_events_event, leader_print_event_counters, null);
            //    evutil_timerclear(&print_events_interval);
            //    print_events_interval.tv_sec = (LEADER_EVENTS_UPDATE_INTERVAL / 1000000);
            //    print_events_interval.tv_usec = (LEADER_EVENTS_UPDATE_INTERVAL % 1000000);
            //    leader_print_event_counters(0, 0, null);
            // #endif

            // Initialize values handler
            // if(vh_init()!= 0) 
            // {
            //    printf("Values handler initialization failed!\n");
            //    return -1;
            // }

            // Reset phase 1 counters
            _phase1PendingCount = 0;
            _phase1ReadyCount = 0;

            // Set so that next p1 to open is _currentInstanceId
            _phase1HighestOpen = _currentInstanceId - 1;

            // Initialize infinite prepare info
            _infinitePrepareOpened = 0;
            _infinitePreparePromisesBitVector = 0;
            _infinitePreparePromisesCount = 0;
            _infinitePrepareStatus = StatusFlag.Empty;

            // Initialize timer and corresponding event for
            // checking timeouts of instances, phase 1
            // evtimer_set(&p1_check_event, leader_periodic_p1_check, null);
            // evutil_timerclear(&p1_check_interval);
            // p1_check_interval.tv_sec = 0; //(P1_TIMEOUT_INTERVAL/3 / 1000000);
            // p1_check_interval.tv_usec = 10000; //(P1_TIMEOUT_INTERVAL/3 % 1000000);

            // Check pending, open new, set next timeout
            LeaderPeriodicPhase1Check();

            // leader_set_next_p1_check();

            // Reset phase 2 counters
            _phase2NextUnusedInstanceId = _currentInstanceId;
            _phase2OpenCount = 0;

            // Initialize timer and corresponding event for
            // checking timeouts of instances, phase 2
            // evtimer_set(&p2_check_event, leader_periodic_p2_check, null);
            // evutil_timerclear(&p2_check_interval);
            // p2_check_interval.tv_sec = (((int)P2_CHECK_INTERVAL) / 1000000);
            // p2_check_interval.tv_usec = ((P2_CHECK_INTERVAL) % 1000000);
            LeaderSetNextPhase2Check();

            Log(1, "Leader is ready");
        }

        private void LeaderShutdown()
        {
            Log(0, "Proposer {0} dropping leadership", Id);

            // evtimer_del(&p1_check_event);
            // evtimer_del(&p2_check_event);
            if (Configuration.LeaderEventsUpdateInterval)
            {
                // evtimer_del(&print_events_event);
            }

            // Iterate over currently open instances 
            for (var i = _currentInstanceId; i <= _phase1HighestOpen; i++) 
            {
                var ii = _records[GetRecordIndex(i)];
                if (ii.Status != StatusFlag.Phase2Completed && ii.Phase2Value != null) 
                {
                    // A value was assigned to this instance, but it did 
                    // not complete. Send back to the pending list for now
                    vh_push_back_value(ii.Phase2Value);
                    ii.Phase2Value = null;
                }
            
                // Clear all instances
                ClearInstanceInfo(ii);
            }

            // This will clear all values in the pending list
            // and notify the respective clients
            vh_shutdown();
        }
       
        private void vh_push_back_value(ClientValueWrapper vw)
        {
            throw new NotImplementedException();
        }

        private ClientValueWrapper vh_get_next_pending()
        {
            throw new NotImplementedException();
        }

        private decimal vh_value_compare(ClientValueWrapper phase1Value, ClientValueWrapper phase2Value)
        {
            throw new NotImplementedException();
        }

        private int vh_pending_list_size()
        {
            throw new NotImplementedException();
        }

        private void vh_notify_client(int i, ClientValueWrapper phase2Value)
        {
            throw new NotImplementedException();
        }

        private void vh_shutdown()
        {
            // Delete event
            // event_del(&leader_msg_event);

            // Close socket and free receiver
            // udp_receiver_destroy(for_leader);

            // All values in pending could not be delivered yet.
            // Notify the respective clients
            // vh_value_wrapper* vw;
            // while ((vw = vh_get_next_pending()) != null)
            // {
            //    vh_notify_client(-1, vw);
            //    PAX_FREE(vw);
            // }
        }
        
        internal class ProposerRecord
        {
            public ProposerRecord(int acceptorCount)
            {
                InstanceId = -1;
                Promises = new PrepareResponseMessage[acceptorCount];
            }

            public int InstanceId { get; set; }

            public int BallotId { get; set; }

            public StatusFlag Status { get; set; }

            public int PromiseCount { get; set; }

            public PrepareResponseMessage[] Promises { get; set; }

            public PrepareResponseMessage ReservedPromise { get; set; }

            public ClientValueWrapper Phase1Value { get; set; }

            public ClientValueWrapper Phase2Value { get; set; }

            public int Phase1ValueBallotId { get; set; }

            public DateTime Timeout { get; set; }
        }

        internal class PendingPromiseMessage
        {
            public int FromInstanceId { get; set; }

            public int ToInstanceId { get; set; }

            public DateTime Timer { get; set; }

            public PendingPromiseMessage Next { get; set; }
        }

        internal class ClientValueWrapper
        {
            public string Value { get; set; }

            public ClientValueWrapper Next { get; set; }
        }
    }
}