namespace PaxosSharp.Messages
{
    internal class LeaderMessage : Message
    {
        public LeaderMessage(int currentLeader)
        {
            CurrentLeader = currentLeader;
        }

        public int CurrentLeader { get; private set; }
    }
}