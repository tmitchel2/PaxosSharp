namespace PaxosSharp.Messages
{
    internal class PromiseMessage : Message
    {
        public PromiseMessage(int instanceId, int ballotId, int valueBallotId, string value, int acceptorId)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
            ValueBallotId = valueBallotId;
            Value = value;
            AcceptorId = acceptorId;
        }

        public int InstanceId { get; set; }

        public int BallotId { get; set; }

        public int ValueBallotId { get; set; }

        public string Value { get; set; }

        public int AcceptorId { get; set; }
    }
}