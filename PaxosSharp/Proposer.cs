using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Proposer
    {
        private readonly object _syncLock;
        private readonly int _majority;
        private readonly ProposerRecord[] _records;
        private ClientValueWrapper _clientListHead;
        private ClientValueWrapper _clientListTail;
        private int _highestOpen;
        private int _currentInstanceId;
        private PendingPromiseMessage _phase1PendingListHead;
        private PendingPromiseMessage _phase1PendingListTail;
        private int _phase1ReadyCount;
        private int _phase1PendingCount;
        private ClientValueWrapper _phase2PendingAccept;
        private DateTime _phase2PendingTimer;
        private IDisposable _messageBusSubscription;

        public Proposer(PaxosConfiguration configuration, Learner learner, int id)
        {
            Configuration = configuration;
            Id = id;
            Learner = learner;
            Status = ProcessStatus.Stopped;

            _syncLock = new object();
            _phase1ReadyCount = 0;
            _phase1PendingCount = 0;
            _phase1PendingListHead = null;
            _phase1PendingListTail = null;
            _phase2PendingAccept = null;
            _clientListHead = null;
            _clientListTail = null;
            _highestOpen = -1;
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
            P1Pending,
            P1Ready,
            P2Reserved,
            P2Pending,
            P2Accepted
        }

        public int Id { get; private set; }

        public Learner Learner { get; private set; }

        public PaxosConfiguration Configuration { get; private set; }

        public ProcessStatus Status { get; private set; }

        public void Start()
        {
            if (Status != ProcessStatus.Stopped)
            {
                return;
            }

            Status = ProcessStatus.Starting;

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
            if (message is PromiseMessage)
            {
                OnRecievePromiseMessage(message as PromiseMessage);
                return Task<bool>.Factory.StartNew(() => true);
            }

            return Task<bool>.Factory.StartNew(() => false);
        }

        private void OnRecievePromiseMessage(PromiseMessage promiseMessage)
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
            var lastToOpen = (Configuration.ProposerPreExecWinSize - (_phase1ReadyCount + _phase1PendingCount)) + _highestOpen;

            Log("P{0}: Proposer tries to open instances from {1} to {2}", Id, _highestOpen + 1, lastToOpen);
            for (var i = _highestOpen + 1; i <= lastToOpen; i++)
            {
                // Add prepare to send_buffer
                Configuration.MessageBus.Publish(new PrepareMessage(i, GetNewBallotId()));
                
                // Add to proposer_array
                var record = _records[GetRecordIndex(i)];
                
                // Clears old promises and set status to p1_pending
                InitializeProposerRecord(record, i, GetNewBallotId());
            }
       
            AddToP1PendingList(_highestOpen + 1, lastToOpen);
            _phase1PendingCount += lastToOpen - _highestOpen;
            _highestOpen = lastToOpen;
        }

        private int GetNewBallotId()
        {
            return Configuration.ProposerCountMaxValue + Id;
        }

        private void PeriodicPhase2Check()
        {
            // Phase 2
            var record = _records[GetRecordIndex(_currentInstanceId)];
            if (record.InstanceId == _currentInstanceId)
            {
                if (record.Status == StatusFlag.P1Ready)
                {
                    DeliverNextValue(record);
                }
                else if (record.Status == StatusFlag.P2Reserved)
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
            record.Status = StatusFlag.P2Pending;
            _phase2PendingAccept = wrapper;
            _phase2PendingTimer = DateTime.Now;
        }

        private void SendAcceptMessage(ProposerRecord record, ClientValueWrapper clientValueWrapper)
        {
            Configuration.MessageBus.Publish(new AcceptMessage(record.InstanceId, record.BallotId, clientValueWrapper.Value));
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
            record.Status = StatusFlag.P2Pending;
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
                record.Status = StatusFlag.P1Ready;
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
            record.Status = StatusFlag.P2Reserved;
            record.ReservedPromise = record.Promises[mostRecentBallotIndex];

            Log("P{0}: Instance {1} is now reserved!", Id, record.InstanceId);
            _phase1PendingCount--;
        }

        private bool HandlePromiseMessage(PromiseMessage message, ProposerRecord record)
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
            if (record.Status != StatusFlag.P1Pending) 
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
            var copy = new PromiseMessage(
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

                // if (deliver_callback != NULL)
                // {
                //    deliver_callback(lmsg->value, lmsg->value_size);
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
            Configuration.MessageBus.Publish(new PrepareMessage(_currentInstanceId, record.BallotId));

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
            record.Status = StatusFlag.P1Pending;
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
                if (record.InstanceId != i || record.Status != StatusFlag.P1Pending)
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
                Configuration.MessageBus.Publish(new PrepareMessage(i, record.BallotId));

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

        internal class ProposerRecord
        {
            public ProposerRecord(int acceptorCount)
            {
                InstanceId = -1;
                Promises = new PromiseMessage[acceptorCount];
            }

            public int InstanceId { get; set; }

            public int BallotId { get; set; }

            public StatusFlag Status { get; set; }

            public int PromiseCount { get; set; }

            public PromiseMessage[] Promises { get; set; }

            public PromiseMessage ReservedPromise { get; set; }
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