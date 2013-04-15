using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// Ping message.  Used for failure detection.
    /// </summary>
    [DataContract]
    internal class PingMessage : Message
    {
        public PingMessage(int proposerId, long sequenceId)
        {
            ProposerId = proposerId;
            SequenceId = sequenceId;
        }

        [DataMember(Order = 1)]
        public int ProposerId { get; private set; }

        [DataMember(Order = 2)]
        public long SequenceId { get; private set; }
    }
}