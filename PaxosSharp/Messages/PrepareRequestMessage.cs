namespace PaxosSharp.Messages
{
    /// <summary>
    /// PrepareRequestMessage.
    /// Used in Phase 1a.
    /// Sent from Proposer to the Acceptor.
    /// </summary>
    internal class PrepareRequestMessage : Message
    {
        public PrepareRequestMessage(int instanceId, int ballotId)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
        }

        public int InstanceId { get; private set; }

        public int BallotId { get; private set; }
    }
}