using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// AcceptResponseMessage.
    /// Used in Phase 2b.
    /// Sent from the Acceptor to the Learner.
    /// </summary>
    [DataContract]
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

        [DataMember(Order = 1)]
        public int InstanceId { get; private set; }

        [DataMember(Order = 2)]
        public int BallotId { get; private set; }

        [DataMember(Order = 3)]
        public int ValueBallotId { get; private set; }

        [DataMember(Order = 4)]
        public bool IsFinal { get; private set; }

        [DataMember(Order = 5)]
        public string Value { get; private set; }

        [DataMember(Order = 6)]
        public int AcceptorId { get; private set; }
    }
}