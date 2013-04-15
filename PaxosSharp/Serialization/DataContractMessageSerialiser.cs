using System.IO;
using System.Runtime.Serialization;
using PaxosSharp.Messages;

namespace PaxosSharp.Serialization
{
    public class DataContractMessageSerialiser : IMessageSerialiser
    {
        public void Serialise(Message message, Stream stream)
        {
            var serialiser = new DataContractSerializer(typeof(Message));
            serialiser.WriteObject(stream, message);
        }

        public Message Deserialise(Stream stream)
        {
            var serialiser = new DataContractSerializer(typeof(Message));
            return (Message) serialiser.ReadObject(stream);
        }
    }
}