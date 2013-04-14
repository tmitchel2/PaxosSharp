namespace PaxosSharp.Messages
{
    /// <summary>
    /// AcceptResponseMessage.
    /// Used in Phase 2b.
    /// Sent from the Acceptor to the Learner.
    /// </summary>
    internal class AcceptResponseMessage : Message
    {
        public AcceptResponseMessage(int instanceId, string value, int ballotId, int valueBallotId, bool isFinal, int acceptorId)
        {
            InstanceId = instanceId;
            Value = value;
            BallotId = ballotId;
            ValueBallotId = valueBallotId;
            IsFinal = isFinal;
            AcceptorId = acceptorId;
        }

        public int InstanceId { get; private set; }

        public int BallotId { get; private set; }

        public int ValueBallotId { get; private set; }

        public bool IsFinal { get; private set; }

        public string Value { get; private set; }

        public int AcceptorId { get; private set; }
    }
}