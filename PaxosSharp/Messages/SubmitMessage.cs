using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    [DataContract]
    public class SubmitMessage : Message
    {
        public SubmitMessage(string value)
        {
            Value = value;
        }

        [DataMember(Order = 1)]
        public string Value { get; private set; }
    }
}