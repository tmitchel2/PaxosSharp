namespace PaxosSharp.Messages
{
    /// <summary>
    /// Ping message.  Used for failure detection.
    /// </summary>
    internal class PingMessage : Message
    {
        public PingMessage(int proposerId, long sequenceId)
        {
            ProposerId = proposerId;
            SequenceId = sequenceId;
        }

        public int ProposerId { get; private set; }

        public long SequenceId { get; private set; }
    }
}