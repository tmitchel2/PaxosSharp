using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// PrepareResponseMessage.
    /// Used in Phase 1b.
    /// Sent from the Acceptor to the Proposer.
    /// </summary>
    [DataContract]
    internal class PrepareResponseMessage : Message
    {
        public const int InfinitePrepareInstanceId = -1;

        public PrepareResponseMessage(int instanceId, int ballotId, int valueBallotId, string value, int acceptorId)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
            ValueBallotId = valueBallotId;
            Value = value;
            AcceptorId = acceptorId;
        }

        [DataMember(Order = 1)]
        public int InstanceId { get; set; }

        [DataMember(Order = 2)]
        public int BallotId { get; set; }

        [DataMember(Order = 3)]
        public int ValueBallotId { get; set; }

        [DataMember(Order = 4)]
        public string Value { get; set; }

        [DataMember(Order = 5)]
        public int AcceptorId { get; set; }
    }
}