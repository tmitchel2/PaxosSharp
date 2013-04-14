using System;
using System.Diagnostics;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp
{
    public sealed class Acceptor
    {
        private readonly AcceptorState _infinitRequestRecord;
        private IDisposable _messageBusSubscription;
        private int _highestAcceptedInstanceId;

        public Acceptor(PaxosConfiguration configuration, int id)
        {
            Guard.ArgumentNotNull(configuration, "configuration");
            
            Configuration = configuration;
            Id = id;
            _highestAcceptedInstanceId = 0;
            _infinitRequestRecord = new AcceptorState();
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
                OnReceivePrepareRequestMessage(message as PrepareRequestMessage);
                return new Task<bool>(() => true);
            }
            
            if (message is AcceptRequestMessage)
            {
                OnReceiveAcceptRequestMessage(message as AcceptRequestMessage);
                return new Task<bool>(() => true);
            }

            if (message is RepeatMessage)
            {
                OnReceiveRepeatMessage(message as RepeatMessage);
                return new Task<bool>(() => true);
            }

            return new Task<bool>(() => false);
        }

        private void OnReceivePrepareRequestMessage(PrepareRequestMessage message)
        {
            AcceptorState state;
            if (message.InstanceId == PrepareRequestMessage.InfinitePrepareInstanceId)
            {
                // Try to apply infinite prepare
                state = ApplyInfinitePrepare(message);
            }
            else
            {
                // Retrieve corresponding record
                state = Configuration.NonVolatileStorage.LoadAccept(message.InstanceId);
                
                // Try to apply prepare
                state = ApplyPrepare(message, state);
            }

            // If accepted, send accept_ack
            if (state != null)
            {
                SendPrepareResponseMessage(state);
            }
        }

        private void OnReceiveAcceptRequestMessage(AcceptRequestMessage message)
        {
            // Retrieve correspondin record
            var rec = Configuration.NonVolatileStorage.LoadAccept(message.InstanceId);
            
            // Try to apply accept
            rec = ApplyAccept(message, rec);
            
            // If accepted, send accept_ack
            if (rec != null)
            {
                SendAcceptResponseMessage(rec);
            }
        }

        private void OnReceiveRepeatMessage(RepeatMessage message)
        {
            foreach (var instanceId in message.InstanceIds)
            {
                // Read the corresponding record
                var state = Configuration.NonVolatileStorage.LoadAccept(instanceId);

                // If a value was accepted, send accept_ack
                if (state != null && state.Value != null)
                {
                    SendAcceptResponseMessage(state);
                }
                else
                {
                    Log(TraceEventType.Verbose, "A{0}: Cannot retransmit iid:{1} no value accepted", Id, instanceId);
                }
            }
        }

        private void SendAcceptResponseMessage(AcceptorState rec)
        {
            Configuration.MessageBus.Publish(new AcceptResponseMessage(rec.InstanceId, rec.Value, rec.BallotId, rec.ValueBallotId, rec.IsFinal, Id));
        }

        private void SendPrepareResponseMessage(AcceptorState state)
        {
            Log(TraceEventType.Verbose, "A{0}: Promise for iid:{1} added to buffer", Id, state.InstanceId);
            Configuration.MessageBus.Publish(
                new PrepareResponseMessage(state.InstanceId, state.BallotId, state.ValueBallotId, state.Value, Id));
        }

        private AcceptorState ApplyInfinitePrepare(PrepareRequestMessage message)
        {
            // Given an infinite prepare (phase 1a) request message,
            // will update if the request is valid
            // Return NULL for no changes, the new record if the promise was made
            // We already have a more recent ballot
            if (_infinitRequestRecord.IsFinal && _infinitRequestRecord.BallotId >= message.BallotId)
            {
                Log(TraceEventType.Verbose, "A{0}: Prepare infinite request dropped (ballots curr:{1} recv:{2})", Id, _infinitRequestRecord.BallotId, message.BallotId);
                return null;
            }

            // Record not found or smaller ballot
            // in both cases overwrite and store
            Log(TraceEventType.Verbose, "A{0}: Prepare infinite request is valid (ballot:{1})", Id, message.BallotId);

            // Record does not exist yet
            var rec = _infinitRequestRecord;
            rec.InstanceId = -1;
            rec.BallotId = message.BallotId;
            rec.ValueBallotId = 0;
            rec.IsFinal = true;
            return rec;
        }

        private AcceptorState ApplyPrepare(PrepareRequestMessage message, AcceptorState state)
        {
            // Given a prepare (phase 1a) request message and the
            // corresponding record, will update if the request is valid
            // Return NULL for no changes, the new record if the promise was made
            // We already have a more recent ballot
            if (state != null && state.BallotId >= message.BallotId)
            {
                Log(
                    TraceEventType.Verbose,
                    "A{0}: Prepare request for iid:{1} dropped (ballots curr:{2} recv:{3})",
                    Id,
                    message.InstanceId,
                    state.BallotId,
                    message.BallotId);
                return null;
            }

            // Stored value is final, the instance is closed already
            if (state != null && state.IsFinal)
            {
                Log(
                    TraceEventType.Verbose,
                    "A{0}: Prepare request for iid:{1} dropped (stored value is final)",
                    Id,
                    message.InstanceId);

                return null;
            }

            // Record not found or smaller ballot
            // in both cases overwrite and store
            Log(
                TraceEventType.Verbose,
                "A{0}: Prepare request is valid for iid:{1} (ballot:{2})",
                Id,
                message.InstanceId,
                message.BallotId);

            // Store the updated record
            state = Configuration.NonVolatileStorage.SavePrepare(message, state);
            return state;
        }

        private AcceptorState ApplyAccept(AcceptRequestMessage message, AcceptorState state)
        {
            // We already have a more recent ballot
            if (state != null && state.BallotId > message.BallotId)
            {
                Log(
                    TraceEventType.Verbose,
                    "A{0}: Accept for iid:{1} dropped (ballots curr:{2} recv:{3})",
                    Id,
                    message.InstanceId,
                    state.BallotId,
                    message.BallotId);
                return null;
            }
    
            // Record not found or smaller ballot
            // in both cases overwrite and store
            Log(TraceEventType.Verbose, "A{0}: Accepting for iid:{1} (ballot:{2})", Id, message.InstanceId, message.BallotId);
    
            // Store the updated record
            state = Configuration.NonVolatileStorage.SaveAccept(message);
    
            // Keep track of highest accepted for retransmission
            if (message.InstanceId > _highestAcceptedInstanceId)
            {
                _highestAcceptedInstanceId = message.InstanceId;
                Log(TraceEventType.Verbose, "A{0}: Highest accepted is now iid:{1}", Id, _highestAcceptedInstanceId);
            }

            return state;
        }
        
        private void Log(TraceEventType type, string format, params object[] args)
        {
            Configuration.TraceSource.TraceEvent(type, 0, format, args);
        }
        
        public class AcceptorState
        {
            public int InstanceId { get; set; }

            public int BallotId { get; set; }

            public int ValueBallotId { get; set; }

            public bool IsFinal { get; set; }

            public string Value { get; set; }
        }
    }
}