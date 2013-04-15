using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// AcceptRequestMessage.
    /// Used in Phase 2a.
    /// Sent from the Proposer to the Acceptor.
    /// </summary>
    [DataContract]
    public class AcceptRequestMessage : Message
    {
        public AcceptRequestMessage(int instanceId, int ballotId, string value)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
            Value = value;
        }

        [DataMember(Order = 1)]
        public int InstanceId { get; private set; }

        [DataMember(Order = 2)]
        public int BallotId { get; private set; }

        [DataMember(Order = 3)]
        public string Value { get; private set; }
    }
}