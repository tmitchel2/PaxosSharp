using System;
using System.Diagnostics;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Acceptor
    {
        private readonly AcceptorRecord[] _records;
        private IDisposable _messageBusSubscription;

        public Acceptor(PaxosConfiguration configuration, int id)
        {
            Configuration = configuration;
            Id = id;
            _records = new AcceptorRecord[Configuration.AcceptorCount];
            for (var i = 0; i < _records.Length; i++)
            {
                _records[i] = new AcceptorRecord { InstanceId = -1, BallotId = -1, Value = null };
            }

            Log(TraceEventType.Verbose, "A{0}: Init completed", Id);
        }

        public int Id { get; private set; }

        public PaxosConfiguration Configuration { get; private set; }

        public void Start()
        {
            _messageBusSubscription = Configuration.MessageBus.Subscribe(OnReceiveMessage);
        }

        public void Stop()
        {
            _messageBusSubscription.Dispose();
        }

        private Task<bool> OnReceiveMessage(Message message)
        {
            if (message is PrepareRequestMessage)
            {
                OnReceivePrepareMessage(message as PrepareRequestMessage);
                return new Task<bool>(() => true);
            }
            
            if (message is AcceptRequestMessage)
            {
                OnReceiveAcceptMessage(message as AcceptRequestMessage);
                return new Task<bool>(() => true);
            }

            if (message is RepeatMessage)
            {
                OnReceiveLearnerSyncMessage(message as RepeatMessage);
                return new Task<bool>(() => true);
            }

            return new Task<bool>(() => false);
        }

        private void OnReceivePrepareMessage(PrepareRequestMessage message)
        {
            var record = _records[GetRecordIndex(message.InstanceId)];

            // Handle a new instance
            // possibly get rid of an old instance
            if (message.InstanceId > record.InstanceId)
            {
                record.InstanceId = message.InstanceId;
                record.BallotId = message.InstanceId;
                record.ValueBallotId = -1;
                record.Value = null;

                Log(
                    TraceEventType.Verbose,
                    "A{0}: Promising for instance {1} with ballot {2}, never seen before",
                    Id,
                    message.InstanceId,
                    message.BallotId);
                UpdateNonVolatileRecord(record);
                SendPromise(record);
            }

            // Handle a previously written instance
            if (message.InstanceId == record.InstanceId)
            {
                if (message.BallotId <= record.BallotId)
                {
                    Log(TraceEventType.Verbose, "A{0}: Ignoring prepare for instance {1} with ballot {2}, already promised to {3}", Id, message.InstanceId, message.BallotId, record.BallotId);
                    return;
                }

                // Answer if ballot is greater than last one
                Log(TraceEventType.Verbose, "A{0}: Promising for instance {1} with ballot {2}, previously promised to {3}", Id, message.InstanceId, message.BallotId, record.BallotId);
                record.BallotId = message.BallotId;
                UpdateNonVolatileRecord(record);
                SendPromise(record);
            }

            // Record was overwritten in memory, retrieve from disk
            if (message.InstanceId < record.InstanceId)
            {
                record = RetrieveNonVolatileRecord(message.InstanceId);
                if (record == null)
                {
                    // No non volatile record
                    record = new AcceptorRecord
                        {
                            InstanceId = message.InstanceId,
                            BallotId = -1,
                            ValueBallotId = -1,
                            Value = null
                        };
                }

                if (message.BallotId > record.BallotId)
                {
                    record.BallotId = message.BallotId;
                    UpdateNonVolatileRecord(record);
                    SendPromise(record);
                }
                else
                {
                    Log(TraceEventType.Verbose,
                        "A{0}: Ignoring prepare for instance {1} with ballot {2}, already promised to {3} [info from disk]",
                        Id,
                        message.InstanceId,
                        message.BallotId,
                        record.BallotId);
                }
            }
        }

        private void OnReceiveAcceptMessage(AcceptRequestMessage message)
        {
            var record = _records[GetRecordIndex(message.InstanceId)];

            // Found record previously written
            if (message.InstanceId == record.InstanceId)
            {
                if (message.BallotId >= record.BallotId)
                {
                    Log(TraceEventType.Verbose,"A{0}: Accepting value for instance {1} with ballot {2}", Id, message.InstanceId, message.BallotId);
                    ApplyAccept(record, message);
                    SendLearnMessage(record);
                    return;
                }
                else
                {
                    Log(TraceEventType.Verbose,"A{0}: Ignoring accept for instance {1} with ballot {2}, already given to ballot {3}", Id, message.InstanceId, message.BallotId, record.BallotId);
                    return;
                }
            }

            // Record not found in records
            if (message.InstanceId > record.InstanceId)
            {
                Log(TraceEventType.Verbose,"A{0}: Accepting value instance {1} with ballot {2}, never seen before", Id, message.InstanceId, message.BallotId);
                ApplyAccept(record, message);
                SendLearnMessage(record);
                return;
            }

            // Record was overwritten in records
            // We must scan the logfile before accepting or not
            if (message.InstanceId < record.InstanceId)
            {
                record = RetrieveNonVolatileRecord(message.InstanceId);
                if (record == null)
                {
                    Log(TraceEventType.Verbose,"A{0}: Accepting value instance {1} with ballot {2}, never seen before [info from disk]", Id, message.InstanceId, message.BallotId);
                    record = new AcceptorRecord { };
                    ApplyAccept(record, message);
                    SendLearnMessage(record);
                    return;
                }

                if (message.BallotId >= record.BallotId)
                {
                    Log(TraceEventType.Verbose,"A{0}: Accepting value instance {1} with ballot {2} [info from disk]", Id, message.InstanceId, message.BallotId);
                    ApplyAccept(record, message);
                    SendLearnMessage(record);
                    return;
                }
                else
                {
                    Log(TraceEventType.Verbose,"A{0}: Ignoring accept for instance {1} with ballot {2}, already given to ballot {3} [info from disk]", Id, message.InstanceId, message.BallotId, record.BallotId);
                    return;
                }
            }
        }

        private void OnReceiveLearnerSyncMessage(RepeatMessage message)
        {
            foreach (var instanceId in message.InstanceIds)
            {
                SyncLearner(instanceId);
            }
        }

        private void ApplyAccept(AcceptorRecord record, AcceptRequestMessage message)
        {
            // Update record
            record.InstanceId = message.InstanceId;
            record.BallotId = message.BallotId;
            record.ValueBallotId = message.BallotId;
            record.Value = message.Value;

            // Write to non volatile storage
            UpdateNonVolatileRecord(record);
            SyncNonVolatileRecord();
            Log(TraceEventType.Verbose, "A{0}: Accept for iid:{1} applied", Id, record.InstanceId);
        }

        private void SendLearnMessage(AcceptorRecord record)
        {
            Log(TraceEventType.Verbose, "A{0}: Sending learn for iid:{1}", Id, record.InstanceId);
            Configuration.MessageBus.Publish(new AcceptResponseMessage(record.InstanceId, record.Value, record.BallotId, Id));
        }

        private void SyncLearner(int instanceId)
        {
            var record = _records[GetRecordIndex(instanceId)];

            // Found record in memory
            if (instanceId == record.InstanceId)
            {
                // A value was accepted
                if (record.Value != null)
                {
                    // Answering to learner sync for instance
                    SendLearnMessage(record);
                }

                Log(TraceEventType.Verbose,"A{0}: Ignoring lsync for instance {1}, record present but no value accepted", Id, instanceId);
                return;
            }

            // Record was overwritten in acceptor array
            // We must scan the logfile before answering
            if (instanceId < record.InstanceId)
            {
                record = RetrieveNonVolatileRecord(instanceId);

                // A record was found, we accepted something
                if (record != null)
                {
                    // Answering to learner sync for instance [info from disk]
                    SendLearnMessage(record);
                }
                else
                {
                    Log(TraceEventType.Verbose,"A{0}: Ignoring lsync for instance {1}, no value accepted [info from disk]", Id, instanceId);            
                }
            }

            Log(TraceEventType.Verbose,"A{0}: Ignoring lsync for instance {1}, no info is available", Id, instanceId);
        }

        private AcceptorRecord RetrieveNonVolatileRecord(int instanceId)
        {
            return null;
        }

        private void SendPromise(AcceptorRecord record)
        {
            Log(TraceEventType.Verbose, "A{0}: Promise for iid:{1} added to buffer", Id, record.InstanceId);
            Configuration.MessageBus.Publish(
                new PrepareResponseMessage(record.InstanceId, record.BallotId, record.ValueBallotId, record.Value, Id));
        }

        private void UpdateNonVolatileRecord(AcceptorRecord record)
        {
        }

        private void SyncNonVolatileRecord()
        {
        }

        private int GetRecordIndex(int instanceId)
        {
            return instanceId & (_records.Length - 1);
        }

        private void Log(TraceEventType type, string format, params object[] args)
        {
            Configuration.TraceSource.TraceEvent(type, 0, format, args);
        }

        internal class AcceptorRecord
        {
            public int InstanceId { get; set; }

            public int BallotId { get; set; }

            public int ValueBallotId { get; set; }

            public string Value { get; set; }
        }
    }
}