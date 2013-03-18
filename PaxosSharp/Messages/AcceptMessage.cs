namespace PaxosSharp.Messages
{
    internal class AcceptMessage : Message
    {
        public AcceptMessage(int instanceId, int ballotId, string value)
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