using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp;
using PaxosSharp.Messages;

namespace PaxosSharpTests
{
    internal class SimpleMessageBus : IMessageBus
    {
        private readonly ConcurrentDictionary<Func<Message, Task<bool>>, object> _callbacks;

        public SimpleMessageBus()
        {
            _callbacks = new ConcurrentDictionary<Func<Message, Task<bool>>, object>();
        }
        
        public IDisposable Subscribe(Func<Message, Task<bool>> callback)
        {
            return new SubscriptionToken(_callbacks, callback);
        }

        public void Publish(Message message)
        {
            OnReceive(message);
        }

        private void OnReceive(Message message)
        {
            foreach (var callback in _callbacks.Keys)
            {
                Task.Factory.StartNew(() => callback(message));
            }
        }

        internal class SubscriptionToken : IDisposable
        {
            private readonly ConcurrentDictionary<Func<Message, Task<bool>>, object> _callbacks;
            private readonly Func<Message, Task<bool>> _callback;

            public SubscriptionToken(ConcurrentDictionary<Func<Message, Task<bool>>, object> callbacks, Func<Message, Task<bool>> callback)
            {
                _callbacks = callbacks;
                _callback = callback;
                if (!_callbacks.TryAdd(callback, null))
                {
                    throw new Exception();
                }
            }
            
            public void Dispose()
            {
                object value;
                if (!_callbacks.TryRemove(_callback, out value))
                {
                    throw new Exception();
                }
            }
        }
    }
}