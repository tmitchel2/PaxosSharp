using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// PrepareRequestMessage.
    /// Used in Phase 1a.
    /// Sent from Proposer to the Acceptor.
    /// </summary>
    [DataContract]
    public class PrepareRequestMessage : Message
    {
        public const int InfinitePrepareInstanceId = -1;

        public PrepareRequestMessage(int instanceId, int ballotId)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
        }

        [DataMember(Order = 1)]
        public int InstanceId { get; private set; }

        [DataMember(Order = 2)]
        public int BallotId { get; private set; }
    }
}