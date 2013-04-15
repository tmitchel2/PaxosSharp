using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    [DataContract]
    internal class LeaderMessage : Message
    {
        public LeaderMessage(int currentLeader)
        {
            CurrentLeader = currentLeader;
        }

        [DataMember(Order = 1)]
        public int CurrentLeader { get; private set; }
    }
}