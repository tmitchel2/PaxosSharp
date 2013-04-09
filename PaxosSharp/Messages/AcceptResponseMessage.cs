namespace PaxosSharp.Messages
{
    /// <summary>
    /// AcceptResponseMessage.
    /// Used in Phase 2b.
    /// Sent from the Acceptor to the Learner.
    /// </summary>
    internal class AcceptResponseMessage : Message
    {
        public AcceptResponseMessage(int instanceId, string value, int ballotId, int acceptorId)
        {
            InstanceId = instanceId;
            Value = value;
            BallotId = ballotId;
            AcceptorId = acceptorId;
        }

        public string Value { get; private set; }

        public int InstanceId { get; private set; }

        public int BallotId { get; private set; }

        public int AcceptorId { get; private set; }
    }
}