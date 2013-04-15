using System;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp.MessageBus
{
    public class UdpMulticastMessageBus : IMessageBus
    {
        public IDisposable Subscribe(Func<Message, Task<bool>> callback)
        {
            throw new NotImplementedException();
        }

        public void Publish(Message message)
        {
            throw new NotImplementedException();
        }
    }
}