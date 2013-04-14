using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Proposer
    {
        private readonly int _majority;
        private readonly ProposerState[] _states;
        private readonly object _pendingListLock;

        private int _currentInstanceId;
        private int _leaderId;
        private long _alivePingSequenceNo;
        private IDisposable _messageBusSubscription;

        // Phase 1 info
        private int _phase1PendingCount;
        private int _phase1ReadyCount;
        private int _phase1HighestOpen;

        // Phase 2 info
        private int _phase2NextUnusedInstanceId;
        private int _phase2OpenCount;

        // Infinite prepare info
        private int _infinitePrepareOpened;
        private BitVector32 _infinitePreparePromisesBitVector;
        private object _infinitePreparePromisesBitVectorLocker;
        private int _infinitePreparePromisesCount;
        private StatusFlag _infinitePrepareStatus;
        private int _infinitePrepareBallotId;

        // Value handler fields
        private int _valueListSize;
        private ClientValueWrapper _valueListHead;
        private ClientValueWrapper _valueListTail;
        private int _valueDroppedCount;
        
        public Proposer(PaxosConfiguration configuration, Learner learner, int id)
        {
            Guard.ArgumentNotNull(configuration, "configuration");
            Guard.ArgumentNotNull(learner, "learner");
            Guard.ArgumentOutOfRange(id, 0, configuration.ProposerCountMaxValue, "id");

            Configuration = configuration;
            Id = id;
            Learner = learner;
            Learner.Delivered += OnLearntValueDelivered;
            Status = ProcessStatus.Stopped;

            _pendingListLock = new object();
            _infinitePreparePromisesBitVectorLocker = new object();
            _leaderId = 0;
            _currentInstanceId = 1;
            _majority = (Configuration.AcceptorCount / 2) + 1;
            _states = new ProposerState[configuration.ProposerRecordCapacity];
            for (var i = 0; i < _states.Length; i++)
            {
                _states[i] = new ProposerState();
            }

            Log(TraceEventType.Verbose, "P{0}: Init completed", Id);
        }

        internal enum StatusFlag
        {
            Empty,
            Phase1Pending,
            Phase1Ready,
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
                StartAsLeader();
            }

            // Proposer and LeaderProposer initialisation
            Task.Factory.StartNew(FailureDetectionLoop);
            _messageBusSubscription = Configuration.MessageBus.Subscribe(OnRecieveMessage);

            Status = ProcessStatus.Running;
        }

        public void Stop()
        {
            Status = ProcessStatus.Stopping;
            if (IsLeader)
            {
                StopAsLeader();
            }

            _messageBusSubscription.Dispose();
        }
        
        public void SubmitValue(string value)
        {
            Configuration.MessageBus.Publish(new SubmitMessage(value));
        }

        private static void LeaderSetExpiration(ProposerState state, TimeSpan interval)
        {
            state.Timeout = DateTime.Now + interval;
        }

        private static bool IsLeaderExpired(DateTime deadline, DateTime now)
        {
            return now > deadline;
        }
        
        private static void ClearInstanceInfo(ProposerState state)
        {
            state.InstanceId = 0;
            state.Status = StatusFlag.Empty;
            state.BallotId = 0;
            state.Phase1ValueBallotId = 0;
            state.PromisesBitVector = new BitVector32(0);
            state.PromiseCount = 0;
            state.Phase1Value = null;
            state.Phase2Value = null;
        }

        private static ClientValueWrapper WrapValue(string value)
        {
            return new ClientValueWrapper { Value = value };
        }

        private void StopAsLeader()
        {
            // All values in pending could not be delivered yet.
            // Notify the respective clients
            ClientValueWrapper vw;
            while ((vw = PopPendingValue()) != null)
            {
                NotifyClient(-1, vw);
            }
        }
        
        private async void LeaderPrintEventCounters()
        {
            Log(TraceEventType.Verbose, "-----------------------------------------------");
            Log(TraceEventType.Verbose, "current_iid:{0}", _currentInstanceId);
            Log(TraceEventType.Verbose, "Phase 1_____________________:");
            Log(TraceEventType.Verbose, "p1_timeout:{0}", 0);
            Log(TraceEventType.Verbose, "p1_info.pending_count:{0}", _phase1PendingCount);
            Log(TraceEventType.Verbose, "p1_info.ready_count:{0}", _phase1ReadyCount);
            Log(TraceEventType.Verbose, "p1_info.highest_open:{0}", _phase1HighestOpen);
            Log(TraceEventType.Verbose, "Phase 2_____________________:");
            Log(TraceEventType.Verbose, "p2_timeout:{0}", 0);
            Log(TraceEventType.Verbose, "p2_waits_p1:{0}", 0);
            Log(TraceEventType.Verbose, "p2_info.open_count:{0}", _phase2OpenCount);
            Log(TraceEventType.Verbose, "p2_info.next_unused_iid:{0}", _phase2NextUnusedInstanceId);
            Log(TraceEventType.Verbose, "Misc._______________________:");
            Log(TraceEventType.Verbose, "_valueDroppedCount:{0}", _valueDroppedCount);
            Log(TraceEventType.Verbose, "-----------------------------------------------\n");

            await Task.Delay(Configuration.LeaderEventsUpdateInterval);
            if (IsLeader && (Status == ProcessStatus.Starting || Status == ProcessStatus.Running))
            {
                // Recurse
                LeaderPrintEventCounters();
            }
        }

        private void Log(TraceEventType eventType, string format, params object[] args)
        {
            Configuration.TraceSource.TraceEvent(eventType, 0, format, args);
        }

        private void OnLearntValueDelivered(object sender, Learner.LearntValue value)
        {
            Log(TraceEventType.Verbose, "P{0}: Instance iid:{1} delivered to proposer", Id, value.InstanceId);
            
            // If leader, take the appropriate action
            if (IsLeader)
            {
                LeaderDeliver(value);
            }
    
            _currentInstanceId = value.InstanceId + 1;
        }

        private Task<bool> OnRecieveMessage(Message message)
        {
            if (message is SubmitMessage)
            {
                OnRecieveSubmitMessage(message as SubmitMessage);
                return Task<bool>.Factory.StartNew(() => true);
            }

            if (message is PrepareResponseMessage)
            {
                OnRecievePrepareResponseMessage(message as PrepareResponseMessage);
                return Task<bool>.Factory.StartNew(() => true);
            }
            
            if (message is LeaderMessage)
            {
                OnRecieveLeaderMessage(message as LeaderMessage);
                return Task<bool>.Factory.StartNew(() => true);
            }

            return Task<bool>.Factory.StartNew(() => false);
        }

        private void OnRecieveSubmitMessage(SubmitMessage message)
        {
            if (!IsLeader)
            {
                return;
            }

            PushPendingValue(message.Value);
        }

        private void OnRecieveLeaderMessage(LeaderMessage message)
        {
            if (IsLeader && message.CurrentLeader != Id)
            {
                // Some other proposer was nominated leader instead of this one, 
                // step down from leadership
                InitialiseAsNonLeader();
            }
            else if (!IsLeader && message.CurrentLeader == Id)
            {
                // This proposer has just been promoted to leader
                StartAsLeader();
            }

            _leaderId = message.CurrentLeader;
        }

        private void OnRecievePrepareResponseMessage(PrepareResponseMessage message)
        {
            // Ignore if not the current leader
            if (!IsLeader)
            {
                return;
            }

            var isInstanceReady = false;
            if (message.InstanceId == PrepareResponseMessage.InfinitePrepareInstanceId)
            {
                // Infinite prepare ack
                if (HandleInfinitePrepareResponseMessage(message))
                {
                    isInstanceReady = true;
                }
            }
            else 
            {
                // Normal prepare ack
                if (HandlePrepareResponseMessage(message))
                {
                    isInstanceReady = true;
                }
            }

            Log(
                TraceEventType.Verbose,
                "P{0}: Some instances just completed phase 1. Status: p1_pending_count:{1}, p1_ready_count:{2}",
                Id,
                _phase1PendingCount,
                _phase1ReadyCount);
            
            // Some instance completed phase 1
            if (isInstanceReady) 
            {
                // Send a value for p2 timed-out that 
                // had to go trough phase 1 again
                LeaderOpenInstancesPhase2Expired();

                // try to send a value in phase 2
                // for new instances
                LeaderOpenInstancesPhase2New();
            }
        }

        private bool HandlePrepareResponseMessage(PrepareResponseMessage message)
        {
            var state = _states[GetRecordIndex(message.InstanceId)];
            
            // If not p1_pending, drop
            if (state.Status != StatusFlag.Phase1Pending) 
            {
                Log(TraceEventType.Verbose, "P{0}: Promise dropped, iid:{1} not pending", Id, message.InstanceId);
                return false;
            }
    
            // If not our ballot, drop
            if (message.BallotId != state.BallotId)
            {
                Log(TraceEventType.Verbose, "P{0}: Promise dropped, iid:{1} not our ballot", Id, message.InstanceId);
                return false;
            }
    
            // Save the acknowledgement from this acceptor
            // Takes also care of value that may be there
            SavePrepareResponseMessage(state, message);
    
            // Not a majority yet for this instance
            if (state.PromiseCount < _majority)
            {
                Log(TraceEventType.Verbose, "P{0}: Not yet a quorum for iid:{1}", Id, message.InstanceId);
                return false;
            }
    
            // Quorum reached!
            state.Status = StatusFlag.Phase1Ready;
            _phase1PendingCount -= 1;
            _phase1ReadyCount += 1;
            Log(TraceEventType.Verbose, "P{0}: Quorum for iid:{1} reached", Id, message.InstanceId);
            return true;
        }

        private void SavePrepareResponseMessage(ProposerState state, PrepareResponseMessage message)
        {
            // Ack from already received!
            if (state.PromisesBitVector[1 << message.AcceptorId])
            {
                Log(
                    TraceEventType.Verbose,
                    "P{0}: Dropping duplicate promise from:{1}, iid:{2}",
                    Id,
                    message.AcceptorId,
                    state.InstanceId);
                return;
            }
    
            // promise is new
            var bitVector = state.PromisesBitVector;
            bitVector[1 << message.AcceptorId] = true;
            state.PromisesBitVector = bitVector;
            state.PromiseCount++;
            Log(
                TraceEventType.Verbose,
                "P{0}: Received valid promise from:%d, iid:%ld",
                Id,
                message.AcceptorId,
                state.InstanceId);
    
            // Promise contains no value
            if (message.Value == null)
            {
                Log(TraceEventType.Verbose, "P{0}: No value in promise", Id);
                return;
            }

            // Promise contains a value
            // Our value has same or greater ballot
            if (state.Phase1ValueBallotId >= message.ValueBallotId)
            {
                // Keep the current value
                Log(TraceEventType.Verbose, "P{0}: Included value is ignored (cause:value_ballot)", Id);
                return;
            }
    
            // Ballot is greater but the value is actually the same
            if (state.Phase1Value != null && state.Phase1Value.Value == message.Value)
            {
                // Just update the value ballot
                Log(TraceEventType.Verbose, "P{0}: Included value is the same with higher value_ballot", Id);
                state.Phase1ValueBallotId = message.ValueBallotId;
                return;
            }
    
            // Save the received value 
            state.Phase1Value = WrapValue(message.Value);
            state.Phase1ValueBallotId = message.ValueBallotId;
            Log(TraceEventType.Verbose, "P{0}: Value in promise saved", Id);
        }
        
        private bool HandleInfinitePrepareResponseMessage(PrepareResponseMessage message)
        {
            // If not p1_pending, drop
            if (_infinitePrepareStatus != StatusFlag.Phase1Pending)
            {
                Log(TraceEventType.Verbose, "P{0}: Promise dropped, infinite prepare not requested", Id);
                return false;
            }

            // If not our ballot, drop
            if (message.BallotId != _infinitePrepareBallotId)
            {
                Log(TraceEventType.Verbose, "P{0}: Promise dropped, iid:{1} not our ballot", Id, message.InstanceId);
                return false;
            }

            // Save the acknowledgement from this acceptor
            // Ack from already received!
            if (_infinitePreparePromisesBitVector[1 << message.AcceptorId])
            {
                Log(
                    TraceEventType.Verbose,
                    "P{0}: Dropping duplicate promise from:{1}, infinite prepare request",
                    Id,
                    message.AcceptorId);
                return false;
            }

            // Promise is new
            lock (_infinitePreparePromisesBitVectorLocker)
            {
                _infinitePreparePromisesBitVector[1 << message.AcceptorId] = true;
                _infinitePreparePromisesCount++;
                Log(
                    TraceEventType.Verbose,
                    "P{0}: Received valid promise from:{1}, infinite prepare request",
                    Id,
                    message.AcceptorId);

                // Not a majority yet for this instance
                if (_infinitePreparePromisesCount < _majority)
                {
                    Log(TraceEventType.Verbose, "P{0}: Not yet a quorum for infinite prepare request", Id);
                    return false;
                }
            }

            // Quorum reached!
            _infinitePrepareStatus = StatusFlag.Phase1Ready;

            var activeCount = _phase1PendingCount + _phase1ReadyCount;
            var toOpen = Configuration.ProposerPreExecWinSize - activeCount;
            _phase1ReadyCount += toOpen;
            Log(TraceEventType.Verbose, "P{0}: Quorum for infinite prepare request reached\n", Id);
            return true;
        }
        
        private int GetFirstBallotId()
        {
            return Configuration.ProposerCountMaxValue + Id;
        }

        private int GetNextBallotId(int ballotId)
        {
            return ballotId + Configuration.ProposerCountMaxValue;
        }
        
        private async void FailureDetectionLoop()
        {
            _alivePingSequenceNo++;
            Configuration.MessageBus.Publish(new PingMessage(Id, _alivePingSequenceNo));

            await Task.Delay(Configuration.FailureDetectionInterval);
            if (Status == ProcessStatus.Starting || Status == ProcessStatus.Running)
            {
                // Recurse
                FailureDetectionLoop();
            }
        }
        
        private int GetRecordIndex(int instanceId)
        {
            return instanceId % _states.Length;
        }
        
        private void LeaderCheckPhase1Pending()
        {
            // Get current time for checking expired    
            var now = DateTime.Now;
            Log(TraceEventType.Verbose, "P{0}: Checking pending phase 1 from {1} to {2}", Id, _currentInstanceId, _phase1HighestOpen);
            for (var id = _currentInstanceId; id <= _phase1HighestOpen; id++)
            {
                // Get instance from state array
                var record = _states[GetRecordIndex(id)];

                // assert(record.iid == iid_iterator);
                // Still pending . it's expired
                if (record.Status == StatusFlag.Phase1Pending && IsLeaderExpired(record.Timeout, now))
                {
                    Log(TraceEventType.Verbose, "P{0}: Phase 1 of instance {1} expired!", Id, record.InstanceId);

                    // Reset fields used for previous phase 1
                    record.PromisesBitVector = new BitVector32();
                    record.PromiseCount = 0;
                    record.Phase1ValueBallotId = 0;
                    record.Phase1Value = null;

                    // Ballot is incremented
                    record.BallotId = GetNextBallotId(record.BallotId);

                    // Send prepare to acceptors
                    Configuration.MessageBus.Publish(new PrepareRequestMessage(record.InstanceId, record.BallotId));
                    LeaderSetExpiration(record, Configuration.Phase1Timeout);
                }
            }
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
                // Send prepare to acceptors
                Configuration.MessageBus.Publish(
                    new PrepareRequestMessage(PrepareRequestMessage.InfinitePrepareInstanceId, GetFirstBallotId()));

                // Keep track of pending count
                _infinitePrepareStatus = StatusFlag.Phase1Pending;
                _infinitePrepareBallotId = GetFirstBallotId();
                Log(TraceEventType.Verbose, "P{0}: Opened {1} new instances", Id, 1);
            }
        }

        private async void LeaderPeriodicPhase1Check()
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
            await Task.Delay(Configuration.Phase1TimeoutCheckInterval);
            if (IsLeader && (Status == ProcessStatus.Starting || Status == ProcessStatus.Running))
            {
                // Recurse
                LeaderPeriodicPhase1Check();
            }
        }

        private async void LeaderPeriodicPhase2Check()
        {
            // from current to highest open, check deadline
            // if instances in status p2_pending
            var now = DateTime.Now; 
            for (var i = _currentInstanceId; i < _phase2NextUnusedInstanceId; i++)
            {
                // Not p2_pending, skip
                var state = _states[GetRecordIndex(i)];
                if (state.Status != StatusFlag.Phase2Pending) 
                {
                    continue;
                }

                // Check if it was closed in the meanwhile
                // (but not delivered yet)
                if (Learner.IsClosed(i))
                {
                    state.Status = StatusFlag.Phase2Completed;
                    _phase2OpenCount -= 1;
                    
                    // The rest (i.e. answering client)
                    // is done when the value is actually delivered
                    Log(TraceEventType.Verbose, "P{0}: Instance {1} closed, waiting for deliver", Id, i);
                    continue;
                }

                // Not expired yet, skip
                if (!IsLeaderExpired(state.Timeout, now)) 
                {
                    continue;
                }

                // Expired and not closed: must restart from phase 1
                state.Status = StatusFlag.Phase1Pending;
                _phase1PendingCount += 1;
                state.BallotId = GetNextBallotId(state.BallotId);

                // Send prepare to acceptors
                Configuration.MessageBus.Publish(new PrepareRequestMessage(state.InstanceId, state.BallotId));
                LeaderSetExpiration(state, Configuration.Phase1Timeout);
                Log(TraceEventType.Verbose, "P{0}: Instance {1} restarts from phase 1", Id, i);
            }

            // Open new instances
            LeaderOpenInstancesPhase2New();

            // Set next check timeout for calling this function
            await Task.Delay(Configuration.Phase2TimeoutCheckInterval);
            if (IsLeader && (Status == ProcessStatus.Starting || Status == ProcessStatus.Running))
            {
                // Recurse
                LeaderPeriodicPhase2Check();
            }
        }
        
        private void LeaderExecutePhase2(ProposerState state)
        {
            if (state.Phase1Value == null && state.Phase2Value == null)
            {
                // Happens when p1 completes without value        
                // Assign a Phase2Value and execute
                state.Phase2Value = PopPendingValue();

                // assert(record.Phase2Value != null);
            }
            else if (state.Phase1Value != null)
            {
                // Only p1 value is present, MUST execute p2 with it
                // Save it as p2 value and execute
                state.Phase2Value = state.Phase1Value;
                state.Phase1Value = null;
                state.Phase1ValueBallotId = 0;
            }
            else if (state.Phase2Value != null)
            {
                // Only p2 valye is present
                // Do phase 2 with it
            }
            else
            {
                // There are both p1 and p2 value
                // Compare them
                var phase1Value = state.Phase1Value != null ? state.Phase1Value.Value : null;
                var phase2Value = state.Phase2Value != null ? state.Phase2Value.Value : null;
                if (String.Compare(phase1Value, phase2Value, StringComparison.Ordinal) == 0)
                {
                    // Same value, just delete Phase1Value
                    state.Phase1Value = null;
                    state.Phase1ValueBallotId = 0;
                }
                else
                {
                    // Different values
                    // Phase2Value is pushed back to pending list
                    PushPendingValueToFront(state.Phase2Value);

                    // Must execute p2 with p1 value
                    state.Phase2Value = state.Phase1Value;
                    state.Phase1Value = null;
                    state.Phase1ValueBallotId = 0;
                }
            }

            // Change instance status
            state.Status = StatusFlag.Phase2Pending;

            // Send the accept request
            Configuration.MessageBus.Publish(
                new AcceptRequestMessage(state.InstanceId, state.BallotId, state.Phase2Value.Value));

            // Set the deadline for this instance
            LeaderSetExpiration(state, Configuration.Phase2Timeout);
        }

        private void LeaderOpenInstancesPhase2Expired()
        {
            // Scan trough p1_ready that have a value
            // assigned or found, execute phase2
            // Start new phase 2 for all instances found in status p1_ready
            // if they are in the range below, phase2 timed-out and 
            // we went successfully trough phase1 again
            var count = 0;
            for (var id = _currentInstanceId; id < _phase2NextUnusedInstanceId; id++)
            {
                var record = _states[GetRecordIndex(id)];

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
            Log(TraceEventType.Verbose, "P{0}: Opened {1} old (timed-out) instances", Id, count);
        }

        private void LeaderOpenInstancesPhase2New()
        {
            // For better batching, opening new instances at the end
            // is preferred when more than 1 can be opened together
            var threshold = (Configuration.ProposerPhase2Concurrency / 3) * 2;
            if (_phase2OpenCount > threshold)
            {
                Log(TraceEventType.Verbose, "P{0}: Skipping Phase2 open, {1} are still active (tresh:{2})", Id, _phase2OpenCount, threshold);
                return;
            }

            Log(TraceEventType.Verbose, "P{0}: Could open {1} p2 instances", Id, Configuration.ProposerPhase2Concurrency - _phase2OpenCount);

            // Start new phase 2 while there is some value from 
            // client to send and we can open more concurrent instances
            var count = 0;
            while ((count + _phase2OpenCount) <= Configuration.ProposerPhase2Concurrency)
            {
                var record = _states[GetRecordIndex(_phase2NextUnusedInstanceId)];

                // assert(record.Phase2Value == null);
                // No value to send for next unused, stop
                if (record.Phase1Value == null && _valueListSize == 0)
                {
                    Log(TraceEventType.Verbose, "P{0}: No value to use for next instance", Id);
                    break;
                }

                var normalReady = record.Status == StatusFlag.Phase1Ready && record.InstanceId != _phase2NextUnusedInstanceId;
                var infReady = record.Status == StatusFlag.Empty && _infinitePrepareStatus == StatusFlag.Phase1Ready;
                var ready = normalReady | infReady;

                // assert(record.Phase2Value == null || inf_ready);
                // Next unused is not ready, stop
                if (!ready)
                {
                    Log(TraceEventType.Verbose, "P{0}: Next instance to use for P2 (iid:{1}) is not ready yet", Id, _phase2NextUnusedInstanceId);
                    Log(TraceEventType.Verbose, "P{0}: {1} {2} {3} {4}", Id, record.Status, _infinitePrepareStatus, record.InstanceId, _phase2NextUnusedInstanceId);
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
            if (count > 0)
            {
                Log(TraceEventType.Verbose, "P{0}: Opened {1} new instances", Id, count);
            }
        }

        private void LeaderDeliver(Learner.LearntValue value)
        {
            Log(TraceEventType.Verbose, "P{0}: Instance {1} delivered to Leader", Id, value.InstanceId);

            // Verify that the value is the one found or associated
            var record = _states[GetRecordIndex(value.InstanceId)];

            // Instance not even initialized, skip
            if (record.InstanceId != value.InstanceId)
            {
                return;
            }

            if (record.Status == StatusFlag.Phase1Ready)
            {
                _phase1PendingCount -= 1;
            }

            if (_phase2NextUnusedInstanceId == value.InstanceId)
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

            var myVal = (record.Phase2Value != null) && (value.Value == record.Phase2Value.Value);
            if (myVal)
            {
                // Our value accepted, notify client that submitted it
                NotifyClient(0, record.Phase2Value);
            }
            else if (record.Phase2Value != null)
            {
                // Different value accepted, push back our value
                PushPendingValueToFront(record.Phase2Value);
                record.Phase2Value = null;
            }

            // Clear current instance
            ClearInstanceInfo(record);

            // If enough instances are ready to 
            // be opened, start phase2 for them
            LeaderOpenInstancesPhase2New();
        }

        private void StartAsLeader()
        {
            Log(TraceEventType.Verbose, "P{0}: Proposer promoted to leader", Id);

            // Initialize values handler
            _valueListSize = 0;
            _valueListHead = null;
            _valueListTail = null;
            _valueDroppedCount = 0;            

            // Reset phase 1 counters
            _phase1PendingCount = 0;
            _phase1ReadyCount = 0;

            // Reset phase 2 counters
            _phase2NextUnusedInstanceId = _currentInstanceId;
            _phase2OpenCount = 0;

            // Set so that next p1 to open is _currentInstanceId
            _phase1HighestOpen = _currentInstanceId - 1;

            // Initialize infinite prepare info
            _infinitePreparePromisesBitVector = new BitVector32();
            _infinitePreparePromisesCount = 0;
            _infinitePrepareStatus = StatusFlag.Empty;
            
            // Start Phase1, Phase2 and EventTrace loops
            Task.Factory.StartNew(LeaderPeriodicPhase1Check);
            Task.Factory.StartNew(LeaderPeriodicPhase2Check);
            if (Configuration.LeaderEventsIsEnabled)
            {
                Task.Factory.StartNew(LeaderPrintEventCounters);
            }

            Log(TraceEventType.Verbose, "P{0}: Leader is ready", Id);
        }

        private void InitialiseAsNonLeader()
        {
            Log(TraceEventType.Verbose, "P{0}: Proposer dropping leadership", Id);
            
            // Iterate over currently open instances 
            for (var i = _currentInstanceId; i <= _phase1HighestOpen; i++)
            {
                var state = _states[GetRecordIndex(i)];
                if (state.Status != StatusFlag.Phase2Completed && state.Phase2Value != null)
                {
                    // A value was assigned to this instance, but it did 
                    // not complete. Send back to the pending list for now
                    PushPendingValueToFront(state.Phase2Value);
                    state.Phase2Value = null;
                }

                // Clear all instances
                ClearInstanceInfo(state);
            }

            // This will clear all values in the pending list
            // and notify the respective clients
            StopAsLeader();
        }

        private void PushPendingValue(string value)
        {
            lock (_pendingListLock)
            {
                // Create wrapper
                if (_valueListSize > Configuration.LeaderProposerQueueCapacity)
                {
                    Log(TraceEventType.Verbose, "P{0}: Value dropped, list is already too long", Id);
                    _valueDroppedCount += 1;
                    return;
                }

                var newValue = WrapValue(value);
                if (_valueListHead == null && _valueListTail == null)
                {
                    // List was empty
                    _valueListHead = newValue;
                    _valueListTail = newValue;
                    _valueListSize = 1;
                }
                else
                {
                    // List was not empty
                    _valueListTail.Next = newValue;
                    _valueListTail = newValue;
                    _valueListSize += 1;
                }
            }

            Log(TraceEventType.Verbose, "P{0}: Value enqueued", Id);
        }
        
        private ClientValueWrapper PopPendingValue()
        {
            lock (_pendingListLock)
            {
                // List is empty
                if (_valueListHead == null)
                {
                    return null;
                }

                // Pop
                var firstValue = _valueListHead;
                _valueListHead = firstValue.Next;

                // Also last element
                if (_valueListTail == firstValue)
                {
                    _valueListTail = null;
                }

                _valueListSize -= 1;
                Log(TraceEventType.Verbose, "P{0}: Popping value", Id);
                return firstValue;
            }
        }

        private void PushPendingValueToFront(ClientValueWrapper value)
        {
            lock (_pendingListLock)
            {
                // Adds as list head
                value.Next = _valueListHead;

                if (_valueListHead == null && _valueListTail == null)
                {
                    // List was empty
                    _valueListHead = value;
                    _valueListTail = value;
                    _valueListSize = 1;
                }
                else
                {
                    // List was not empty
                    _valueListHead = value;
                    _valueListSize += 1;
                }
            }
        }
        
        private void NotifyClient(int result, ClientValueWrapper value)
        {
            // This is a stub for notifying a client that its value
            // could not be delivered (notice that the value may actually
            // be delivered afterward by some other proposer)
            Log(
                TraceEventType.Verbose,
                result != 0 ? "P{0}: Notify client -> Submit failed" : "P{0}: Notify client -> Submit successful",
                Id);
        }

        /// <summary>
        /// Structure used by proposer to store all info relative to a given instance
        /// </summary>
        internal class ProposerState
        {
            public int InstanceId { get; set; }

            public StatusFlag Status { get; set; }

            public int BallotId { get; set; }

            public int PromiseCount { get; set; }

            public BitVector32 PromisesBitVector { get; set; }

            public ClientValueWrapper Phase1Value { get; set; }

            public int Phase1ValueBallotId { get; set; }

            public ClientValueWrapper Phase2Value { get; set; }
            
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