namespace PaxosSharp.Messages
{
    /// <summary>
    /// AcceptRequestMessage.
    /// Used in Phase 2a.
    /// Sent from the Proposer to the Acceptor.
    /// </summary>
    internal class AcceptRequestMessage : Message
    {
        public AcceptRequestMessage(int instanceId, int ballotId, string value)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
            Value = value;
        }

        public int InstanceId { get; private set; }

        public int BallotId { get; private set; }

        public string Value { get; private set; }
    }
}