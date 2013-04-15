using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    [DataContract]
    [KnownType(typeof(AcceptRequestMessage))]
    [KnownType(typeof(AcceptResponseMessage))]
    [KnownType(typeof(LeaderMessage))]
    [KnownType(typeof(PingMessage))]
    [KnownType(typeof(PrepareRequestMessage))]
    [KnownType(typeof(PrepareResponseMessage))]
    [KnownType(typeof(RepeatMessage))]
    [KnownType(typeof(SubmitMessage))]
    public abstract class Message
    {
    }
}