using System.Collections.Concurrent;
using PaxosSharp.Messages;

namespace PaxosSharp.Storage
{
    public class InMemoryNonVolatileStorage : INonVolatileStorage
    {
        private readonly ConcurrentDictionary<int, Acceptor.AcceptorState> _records;

        public InMemoryNonVolatileStorage()
        {
            _records = new ConcurrentDictionary<int, Acceptor.AcceptorState>();
        }

        public Acceptor.AcceptorState SavePrepare(PrepareRequestMessage message, Acceptor.AcceptorState state)
        {
            // Create a new record or update the ballot on the existing
            return _records.AddOrUpdate(
                state.InstanceId,
                i =>
                new Acceptor.AcceptorState
                    {
                        InstanceId = message.InstanceId,
                        BallotId = message.BallotId,
                        ValueBallotId = 0,
                        IsFinal = false,
                        Value = null
                    },
                (i, acceptorState) =>
                    {
                        acceptorState.BallotId = message.BallotId;
                        return acceptorState;
                    });
        }

        public Acceptor.AcceptorState SaveAccept(AcceptRequestMessage message)
        {
            var state = new Acceptor.AcceptorState
                {
                    InstanceId = message.InstanceId,
                    BallotId = message.BallotId,
                    ValueBallotId = message.BallotId,
                    IsFinal = false,
                    Value = message.Value
                };

            return _records.AddOrUpdate(state.InstanceId, state, (i, acceptorState) => state);
        }

        public Acceptor.AcceptorState LoadAccept(int instanceId)
        {
            Acceptor.AcceptorState value;
            _records.TryGetValue(instanceId, out value);
            return value;
        }
    }
}