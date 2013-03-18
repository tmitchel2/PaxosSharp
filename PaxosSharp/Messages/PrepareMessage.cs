namespace PaxosSharp.Messages
{
    internal class PrepareMessage : Message
    {
        public PrepareMessage(int instanceId, int ballotId)
        {
            InstanceId = instanceId;
            BallotId = ballotId;
        }

        public int InstanceId { get; private set; }

        public int BallotId { get; private set; }
    }
}