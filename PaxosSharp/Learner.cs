using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Learner
    {
        [SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:FieldsMustBePrivate", Justification = "Reviewed. Suppression is OK here.")]
        public EventHandler<LearntValue> Delivered;
        private readonly int _majority;
        private readonly LearnerState[] _instanceStates;
        private int _highestSeenInstanceId;
        private int _currentInstanceId;
        private int _highestClosedInstanceId;
        private IDisposable _messageBusSubscription;
        
        public Learner(PaxosConfiguration configuration, int id)
        {
            Guard.ArgumentNotNull(configuration, "configuration");

            Id = id;
            Configuration = configuration;
            Status = ProcessStatus.Stopped;
            _majority = (Configuration.AcceptorCount / 2) + 1;
            Log(TraceEventType.Verbose, "L{0}: Learner starting, acceptors:{1}, majority:{2}", Id, Configuration.AcceptorCount, _majority);
            _highestSeenInstanceId = 1;
            _currentInstanceId = 1;
            _highestClosedInstanceId = 0;
            _instanceStates = new LearnerState[256];
            
            for (var i = 0; i < _instanceStates.Length; i++)
            {
                _instanceStates[i] = new LearnerState(LearnerState.EmptyInstanceId, Configuration.AcceptorCount);
            }
        }

        public int Id { get; private set; }

        public PaxosConfiguration Configuration { get; private set; }

        public ProcessStatus Status { get; private set; }

        public void Start()
        {
            if (Status != ProcessStatus.Stopped)
            {
                return;
            }

            Status = ProcessStatus.Starting;
            _messageBusSubscription = Configuration.MessageBus.Subscribe(OnReceiveMessage);
            Task.Factory.StartNew(MissingMessageCheckLoop, TaskCreationOptions.LongRunning);
            Status = ProcessStatus.Running;
        }

        public void Stop()
        {
            Status = ProcessStatus.Stopping;
            _messageBusSubscription.Dispose();
            Status = ProcessStatus.Stopped;
        }

        internal bool IsClosed(int instanceId)
        {
            var state = _instanceStates[GetRecordIndex(instanceId)];
            return state.InstanceId == instanceId && IsClosed(state);
        }

        private static bool IsClosed(LearnerState record)
        {
            return record.FinalValue != null;
        }

        private static void StoreAcceptResponseMessage(LearnerState state, AcceptResponseMessage message)
        {
            // Stores the AcceptResponseMessage of a given acceptor in the corresponding record 
            // at the appropriate index
            // Assumes state.ResponseMessages[message.AcceptorId] is null already
            state.ResponseMessages[message.AcceptorId] = new AcceptResponseMessage(
                message.InstanceId,
                message.Value,
                message.BallotId,
                message.ValueBallotId,
                message.IsFinal,
                message.AcceptorId);

            // Keep track of most recent accept_ack stored
            state.LastUpdateBallot = message.BallotId;
        }

        private async void MissingMessageCheckLoop()
        {
            // This function is invoked periodically and tries to detect if the learner 
            // missed some message. For example if instance I is closed but instance I-1 
            // it's not, we can't deliver I. So it will ask to the acceptors to repeat 
            // their accepted value
            while (Status == ProcessStatus.Starting || Status == ProcessStatus.Running)
            {
                if (_highestSeenInstanceId > _currentInstanceId + _instanceStates.Length)
                {
                    Log(
                        TraceEventType.Verbose,
                        "L{0}: This learner is lagging behind!!!, highest seen:{1}, highest delivered:{2}",
                        _highestSeenInstanceId,
                        _currentInstanceId - 1);
                    SendRepeatMessages(_currentInstanceId, _highestSeenInstanceId);
                }
                else if (_highestClosedInstanceId > _currentInstanceId)
                {
                    Log(
                        TraceEventType.Verbose,
                        "L{0}: Out of sync, highest closed:{1}, highest delivered:{2}",
                        _highestSeenInstanceId,
                        _currentInstanceId - 1);
                    SendRepeatMessages(_currentInstanceId, _highestClosedInstanceId);
                }

                await Task.Delay(Configuration.LearnerMissingMessageCheckInterval);
            }
        }

        private LearnerState GetInstanceState(int instanceId)
        {
            return _instanceStates[GetRecordIndex(instanceId)];
        }

        private IEnumerable<LearnerState> GetInstanceStates(int fromInstanceId, int toInstanceId)
        {
            for (var i = fromInstanceId; i < toInstanceId; i++)
            {
                yield return GetInstanceState(i);
            }
        }

        private void SendRepeatMessages(int fromInstanceId, int toInstanceId)
        {
            // Get all the states which arent closed
            // and batch them into groups of 50
            var openStateBatches =
                GetInstanceStates(fromInstanceId, toInstanceId)
                .Where(state => !IsClosed(state))
                .GroupByBatchSize(50)
                .ToList();

            // Get the instance ids for each batch
            // and post a RepeatMessage request
            foreach (var openStateBatch in openStateBatches)
            {
                var instanceIds = openStateBatch
                    .Select(state => state.InstanceId)
                    .ToList();

                Configuration.MessageBus.Publish(new RepeatMessage(instanceIds));
            }
        }
        
        private Task<bool> OnReceiveMessage(Message message)
        {
            if (message is AcceptResponseMessage)
            {
                OnReceiveAcceptResponseMessage((AcceptResponseMessage)message);
                return Task<bool>.Factory.StartNew(() => true);
            }

            return Task<bool>.Factory.StartNew(() => false);
        }

        private void OnReceiveAcceptResponseMessage(AcceptResponseMessage message)
        {
            // Keep track of highest seen instance id
            if (message.InstanceId > _highestSeenInstanceId)
            {
                _highestSeenInstanceId = message.InstanceId;
            }

            // Already closed and delivered, ignore message
            if (message.InstanceId < _currentInstanceId)
            {
                Log(TraceEventType.Verbose, "L{0}: Dropping AcceptResponseMessage for instance already delivered:{1}", Id, message.InstanceId);
                return;
            }

            // We are late w.r.t the current iid, ignore message
            // (The instence received is too ahead and will overwrite something)
            if (message.InstanceId >= _currentInstanceId + _instanceStates.Length)
            {
                Log(TraceEventType.Verbose, "L{0}: Dropping AcceptResponseMessage for instance too far in future:{1}", Id, message.InstanceId);
                return;
            }

            // Message is within interesting bounds
            // Update the corresponding record
            var state = _instanceStates[GetRecordIndex(message.InstanceId)];
            if (!UpdateInstanceState(state, message))
            {
                Log(TraceEventType.Verbose, "L{0}: Discarding AcceptResponseMessage for instance:{1}", Id, message.InstanceId);
                return;
            }

            // Message contained some relevant info, 
            // check if instance can be declared closed
            if (!HaveMajorityForInstanceState(state))
            {
                Log(TraceEventType.Verbose, "L{0}: Not yet a majority for instance:{1}", Id, message.InstanceId);
                return;
            }

            // If the closed instance is the current one,
            // Deliver it (and the followings if already closed)
            if (message.InstanceId == _currentInstanceId)
            {
                DeliverNextClosed();
            }
        }

        private void DeliverNextClosed()
        {
            // Invoked when the current_iid is closed.
            // Since other instances may be closed too (curr+1, curr+2), also tries to deliver them
            // Get next instance (last delivered + 1)
            var state = _instanceStates[GetRecordIndex(_currentInstanceId)];
            
            // If closed deliver it and all next closed
            while (IsClosed(state)) 
            {
                // assert(ii->iid == current_iid);
                var message = state.FinalValue;
        
                // Deliver the value trough callback
                var proposerId = message.BallotId % Configuration.ProposerCountMaxValue;
                OnDeliver(new LearntValue(_currentInstanceId, message.Value, message.BallotId, proposerId));
        
                // Move to next instance
                _currentInstanceId++;
        
                // Clear the state
                state.Clear();
        
                // Go on and try to deliver next
                state = _instanceStates[GetRecordIndex(_currentInstanceId)];
            }
        }

        private void OnDeliver(LearntValue value)
        {
            var handler = Delivered;
            if (handler != null)
            {
                handler(this, value);
            }
        }

        private bool HaveMajorityForInstanceState(LearnerState state)
        {
            var count = 0;
            var validIndex = -1;

            // Iterates over stored ResponseMessages
            for (var i = 0; i < Configuration.AcceptorCount; i++)
            {
                var currentResponseMessage = state.ResponseMessages[i];
                
                // No ack from this acceptor, skip
                if (currentResponseMessage == null)
                {
                    continue;
                }

                // Count the ones "agreeing" with the last added
                if (currentResponseMessage.BallotId == state.LastUpdateBallot)
                {
                    validIndex = i;
                    count++;

                    // Special case: an acceptor is telling that
                    // this value is -final-, it can be delivered 
                    // immediately.
                    if (currentResponseMessage.IsFinal)
                    {
                        // For sure >= than quorum...
                        count += Configuration.AcceptorCount;
                        break;
                    }
                }
            }

            // Reached a majority!
            if (count >= _majority)
            {
                Log(TraceEventType.Verbose, "L{0}: Reached quorum, instance:{0} is closed!", state.InstanceId);
                state.FinalValue = state.ResponseMessages[validIndex];
        
                // Keep track of highest closed
                if (state.InstanceId > _highestClosedInstanceId) 
                {
                    _highestClosedInstanceId = state.InstanceId;
                }

                // Yes we have a quorum
                return true;
            }

            // No quorum yet
            return false;
        }
        
        private bool UpdateInstanceState(LearnerState state, AcceptResponseMessage message)
        {
            // Tries to update the state based on the accept_ack received.
            // Returns 0 if the message was discarded because not relevant. 1 if the state changed.

            // Found record that can be cleaned and reused for new
            // instance id record
            if (state.InstanceId == LearnerState.EmptyInstanceId)
            {
                Log(TraceEventType.Verbose, "L{0}: Received first AcceptResponseMessage for instance:{1}", Id, message.InstanceId);
                state.InstanceId = message.InstanceId;
                state.LastUpdateBallot = message.BallotId;
            }

            // Instance closed already, drop 
            if (IsClosed(state))
            {
                Log(TraceEventType.Verbose, "L{0}: Dropping AcceptResponseMessage for closed instance:{1}", Id, message.InstanceId);
                return false;
            }

            // No previous message to overwrite for this acceptor
            if (state.ResponseMessages[message.AcceptorId] == null) 
            {
                Log(TraceEventType.Verbose, "L{0}: Got first AcceptResponseMessage for instance:{1}, acceptor:{2}", Id, state.InstanceId, message.AcceptorId);

                // Save this AcceptResponseMessage
                StoreAcceptResponseMessage(state, message);
                state.LastUpdateBallot = message.BallotId;
                return true;
            }
    
            // There is already a message from the same acceptor
            var previousAcceptResponseMessage = state.ResponseMessages[message.AcceptorId];
    
            // Already more recent info in the record, accept_ack is old
            if (previousAcceptResponseMessage.BallotId >= message.BallotId) 
            {
                Log(TraceEventType.Verbose, "L{0}: Dropping AcceptResponseMessage for instance:{1}, stored ballot is newer or equal", Id, state.InstanceId);
                return false;
            }
    
            // Replace the previous ack since the received ballot is newer
            Log(TraceEventType.Verbose, "L{0}: Overwriting previous AcceptResponseMessage for instance:{1}", Id, state.InstanceId);
            StoreAcceptResponseMessage(state, message);
            state.LastUpdateBallot = message.BallotId;
            return true;
        }

        private long GetRecordIndex(long n)
        {
            return n & (_instanceStates.Length - 1);
        }

        private void Log(TraceEventType type, string format, params object[] args)
        {
            Configuration.TraceSource.TraceEvent(type, 0, format, args);
        }

        public class LearntValue
        {
            internal LearntValue(int instanceId, string value, int ballotId, int proposerId)
            {
                InstanceId = instanceId;
                Value = value;
                BallotId = ballotId;
                ProposerId = proposerId;
            }

            public int InstanceId { get; private set; }

            public string Value { get; private set; }

            public int BallotId { get; private set; }

            public int ProposerId { get; private set; }
        }

        internal class LearnerState
        {
            public const int EmptyInstanceId = 0;

            public LearnerState(int instanceId, int acceptorCount)
            {
                InstanceId = instanceId;
                FinalValue = null;
                LastUpdateBallot = -1;
                ResponseMessages = new AcceptResponseMessage[acceptorCount];
            }

            public int InstanceId { get; set; }

            public int LastUpdateBallot { get; set; }

            public AcceptResponseMessage[] ResponseMessages { get; private set; }

            public AcceptResponseMessage FinalValue { get; set; }

            public void Clear()
            {
                InstanceId = EmptyInstanceId;
                LastUpdateBallot = 0;
                FinalValue = null;
                for (var i = 0; i < ResponseMessages.Length; i++)
                {
                    ResponseMessages[i] = null;
                }
            }

            public void ReuseRecordForInstanceId(int instanceId)
            {
                InstanceId = instanceId;
                FinalValue = null;
                LastUpdateBallot = -1;
                for (var i = 0; i < ResponseMessages.Length; i++)
                {
                    ResponseMessages[i] = null;
                }
            }
        }
    }
}
