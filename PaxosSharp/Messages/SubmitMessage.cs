namespace PaxosSharp.Messages
{
    public class SubmitMessage : Message
    {
        public SubmitMessage(string value)
        {
            Value = value;
        }

        public string Value { get; private set; }
    }
}