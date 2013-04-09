using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Learner
    {
        private readonly int _majority;
        private readonly object _valuesSyncLock;
        private readonly object _recordsSyncLock;
        private readonly LearnerState[] _instanceStates;
        private int _highestSeenInstanceId;
        private int _highestQueuedInstanceId;
        private int _highestClosedInstanceId;
        private LearnerValueWrapper _listHead;
        private LearnerValueWrapper _listTail;
        private IDisposable _messageBusSubscription;

        public Learner(PaxosConfiguration configuration, int id)
        {
            Id = id;
            Configuration = configuration;
            _majority = (Configuration.AcceptorCount / 2) + 1;
            Log(TraceEventType.Verbose,"L{0}: Learner starting, acceptors:{1}, majority:{2}", Id, Configuration.AcceptorCount, _majority);
            _highestSeenInstanceId = -1;
            _highestQueuedInstanceId = -1;
            _highestClosedInstanceId = -1;
            _valuesSyncLock = new object();
            _recordsSyncLock = new object();
            _instanceStates = new LearnerState[256];
            
            for (var i = 0; i < _instanceStates.Length; i++)
            {
                _instanceStates[i] = new LearnerState(-1, Configuration.AcceptorCount);
            }
        }

        public int Id { get; private set; }

        public PaxosConfiguration Configuration { get; private set; }

        public void Start()
        {
            _messageBusSubscription = Configuration.MessageBus.Subscribe(OnReceiveMessage);
            Task.Factory.StartNew(RetransmissionLoop, TaskCreationOptions.LongRunning);
        }

        public void Stop()
        {
            _messageBusSubscription.Dispose();
        }

        public bool TryGetNextValue(out string value)
        {
            var w = GetNextValueWrapper();
            if (w != null)
            {
                value = w.Value;
                return true;
            }

            value = null;
            return false;
        }

        public bool IsClosed(int instanceId)
        {
            var state = _instanceStates[GetRecordIndex(instanceId)];
            return instanceId == state.InstanceId && state.FinalValue != null;
        }

        internal LearnerValueWrapper GetNextValueWrapper()
        {
            LearnerValueWrapper valueWrapper;

            lock (_valuesSyncLock)
            {
                while (_listHead == null)
                {
                    Monitor.Wait(_valuesSyncLock, TimeSpan.FromSeconds(1));
                }

                // List is not empty
                if (_listHead == _listTail)
                {
                    // List has one element
                    Log(TraceEventType.Verbose,"L{0}: Returning only element in learned value list", Id);
                    valueWrapper = _listHead;
                    _listHead = null;
                    _listTail = null;
                }
                else
                {
                    // List has more than one element
                    valueWrapper = _listHead;
                    _listHead = valueWrapper.Next;
                }
            }

            return valueWrapper;
        }

        private void RetransmissionLoop()
        {
            lock (_recordsSyncLock)
            {
                while (true)
                {
                    Monitor.Wait(_recordsSyncLock, Configuration.LearnerSyncInterval);

                    if (_highestSeenInstanceId > _highestQueuedInstanceId)
                    {
                        RequestRetransmission();
                    }
                }
            }
        }
        
        private void RequestRetransmission()
        {
            var instanceIds = new List<int>();
                
            for (var i = _highestQueuedInstanceId + 1; i < _highestSeenInstanceId; i++)
            {
                var record = _instanceStates[GetRecordIndex(i)];

                // Not closed of never seen, request sync
                if ((record.InstanceId == i && !IsClosed(record)) || record.InstanceId < i)
                {
                    Log(TraceEventType.Verbose,"L{0}: Adding {1} to next lsync message", Id, i);
                    instanceIds.Add(i);
                    if (instanceIds.Count >= 50)
                    {
                        Log(
                            TraceEventType.Verbose,
                            "L{0}: Requested {1} lsyncs from {2} to {3}",
                            Id,
                            instanceIds.Count,
                            _highestQueuedInstanceId + 1,
                            _highestSeenInstanceId);
                        Configuration.MessageBus.Publish(new RepeatMessage(instanceIds));
                        instanceIds = new List<int>();
                    }
                }
            }
        }

        private Task<bool> OnReceiveMessage(Message message)
        {
            if (message is AcceptResponseMessage)
            {
                OnReceiveLearnMessage((AcceptResponseMessage)message);
                return Task<bool>.Factory.StartNew(() => true);
            }

            return Task<bool>.Factory.StartNew(() => false);
        }

        private void OnReceiveLearnMessage(AcceptResponseMessage message)
        {
            lock (_recordsSyncLock)
            {
                // Update the highest seen instance id
                if (message.InstanceId > _highestSeenInstanceId)
                {
                    _highestSeenInstanceId = message.InstanceId;
                }

                if (!UpdateRecord(message))
                {
                    Log(TraceEventType.Verbose,"L{0}: Learner discarding learn for instance {1}", Id, message.InstanceId);
                    return;
                }

                if (!HaveMajorityForRecord(message))
                {
                    Log(TraceEventType.Verbose,"L{0}: Not yet a majority for instance {1}", Id, message.InstanceId);
                    return;
                }

                if (message.InstanceId == _highestQueuedInstanceId + 1)
                {
                    Enqueue(message.InstanceId);
                }
            }
        }

        private void Enqueue(int instanceId)
        {
            lock (_valuesSyncLock)
            {
                while (true)
                {
                    var record = _instanceStates[GetRecordIndex(instanceId)];
                    if (!IsClosed(record))
                    {
                        break;
                    }

                    Log(TraceEventType.Verbose,"L{0}: Instance {1} enqueued for client", Id, instanceId);
                    var wrapper = new LearnerValueWrapper
                        {
                            InstanceId = instanceId,
                            BallotId = record.LastUpdateBallot,
                            Value = record.FinalValue
                        };
                    record.FinalValue = null;

                    if (_listHead == null && _listTail == null)
                    {
                        _listHead = wrapper;
                        _listTail = wrapper;
                    }
                    else if (_listHead != null && _listTail != null)
                    {
                        _listTail.Next = wrapper;
                        _listTail = wrapper;
                    }

                    _highestQueuedInstanceId = instanceId;
                    instanceId++;
                }
            }
        }

        private bool HaveMajorityForRecord(AcceptResponseMessage message)
        {
            var record = _instanceStates[GetRecordIndex(message.InstanceId)];
            var count = 0;
            for (var i = 0; i < Configuration.AcceptorCount; i++)
            {
                var storedLearn = record.ResponseMessages[i];
                if (storedLearn == null || storedLearn.Value != message.Value)
                {
                    continue;
                }

                count++;
                if (count >= _majority)
                {
                    Log(TraceEventType.Verbose,"L{0}: Reached majority, instance {1} is now closed!", Id, record.InstanceId);
                    record.LastUpdateBallot = message.BallotId;
                    record.FinalValue = message.Value;

                    if (message.InstanceId > _highestClosedInstanceId)
                    {
                        _highestClosedInstanceId = message.InstanceId;
                    }

                    return true;
                }
            }

            return false;
        }

        private bool UpdateRecord(AcceptResponseMessage message)
        {
            // Not enough storage, drop the message
            if (message.InstanceId >= _highestQueuedInstanceId + _instanceStates.Length)
            {
                Log(TraceEventType.Verbose,"L{0}: Dropping learn for instance too far in future:{1}", Id, message.InstanceId);
                return false;
            }

            // Already seen this message, drop the message
            if (message.InstanceId <= _highestQueuedInstanceId)
            {
                Log(TraceEventType.Verbose,"L{0}: Dropping learn for already enqueued instance:{1}", Id, message.InstanceId);
                return false;
            }

            // Found record that can be cleaned and reused for new
            // instance id record
            var record = _instanceStates[GetRecordIndex(message.InstanceId)];
            if (record.InstanceId != message.InstanceId)
            {
                Log(TraceEventType.Verbose,"L{0}: Received first message for instance:{1}", Id, message.InstanceId);
                record.ReuseRecordForInstanceId(message.InstanceId);
                return AddLearnToRecord(record, message);
            }
            
            // Found closed record 
            if (IsClosed(record))
            {
                Log(TraceEventType.Verbose,"L{0}: Dropping learn for closed instance:{1}", Id, message.InstanceId);
                return false;
            }

            // Found record to update
            return AddLearnToRecord(record, message);
        }

        private bool IsClosed(LearnerState record)
        {
            return record.FinalValue != null;
        }

        private bool AddLearnToRecord(LearnerState record, AcceptResponseMessage message)
        {
            // Check bounds
            if (message.AcceptorId < 0 || message.AcceptorId > Configuration.AcceptorCount)
            {
                return false;
            }

            // First learn message from this acceptor.
            var oldLearn = record.ResponseMessages[message.AcceptorId];
            if (oldLearn == null)
            {
                Log(TraceEventType.Verbose,"L{0}: Got first learn for instance:{1}, acceptor:{2}", Id, record.InstanceId,  message.AcceptorId);
                record.ResponseMessages[message.AcceptorId] = new AcceptResponseMessage(
                    message.InstanceId, message.Value, message.BallotId, message.AcceptorId);
                return true;
            }

            // Out of date message, drop.
            if (oldLearn.BallotId >= message.BallotId)
            {
                Log(TraceEventType.Verbose,"L{0}: Dropping learn for instance:{1}, more recent ballot already seen", Id, record.InstanceId);
                return false;
            }

            // Relevant message, overwrite previous learn
            Log(TraceEventType.Verbose,"L{0}: Overwriting previous learn for instance {1}", Id, record.InstanceId);
            record.ResponseMessages[message.AcceptorId] = new AcceptResponseMessage(
                    message.InstanceId, message.Value, message.BallotId, message.AcceptorId);
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

        internal class LearnerValueWrapper
        {
            internal int InstanceId { get; set; }

            internal string Value { get; set; }

            internal int BallotId { get; set; }

            internal LearnerValueWrapper Next { get; set; }
        }

        internal class LearnerState
        {
            public LearnerState(int instanceId, int acceptorCount)
            {
                InstanceId = instanceId;
                FinalValue = null;
                LastUpdateBallot = -1;
                ResponseMessages = new AcceptResponseMessage[acceptorCount];
            }

            public int InstanceId { get; private set; }

            public AcceptResponseMessage[] ResponseMessages { get; private set; }

            public int LastUpdateBallot { get; set; }

            public string FinalValue { get; set; }

            public void Clear()
            {
                InstanceId = 0;
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
