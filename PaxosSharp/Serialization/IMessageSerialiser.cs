using System.IO;
using PaxosSharp.Messages;

namespace PaxosSharp.Serialization
{
    public interface IMessageSerialiser
    {
        void Serialise(Message message, Stream stream);

        Message Deserialise(Stream stream);
    }
}