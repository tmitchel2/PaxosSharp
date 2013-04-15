using System;
using System.Threading.Tasks;
using PaxosSharp.Messages;

namespace PaxosSharp.MessageBus
{
    public interface IMessageBus
    {
        IDisposable Subscribe(Func<Message, Task<bool>> callback);

        void Publish(Message message);
    }
}