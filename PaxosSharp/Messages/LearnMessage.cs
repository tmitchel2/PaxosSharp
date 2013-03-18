namespace PaxosSharp.Messages
{
    internal class LearnMessage : Message
    {
        public LearnMessage(int instanceId, string value, int ballotId, int acceptorId)
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