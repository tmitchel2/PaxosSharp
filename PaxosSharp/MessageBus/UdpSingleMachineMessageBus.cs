using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using PaxosSharp.Messages;
using PaxosSharp.Serialization;

namespace PaxosSharp.MessageBus
{
    public class UdpSingleMachineMessageBus : IMessageBus
    {
        private const int BasePort = 12000;
        private readonly IMessageSerialiser _serialiser;
        private readonly UdpClient _udpPublishClient;
        private int _subscriptions;

        public UdpSingleMachineMessageBus()
            : this(new DataContractMessageSerialiser())
        {
        }

        public UdpSingleMachineMessageBus(IMessageSerialiser serialiser)
        {
            _serialiser = serialiser;
            _udpPublishClient = new UdpClient();
        }

        public IDisposable Subscribe(Func<Message, Task<bool>> callback)
        {
            var port = BasePort + Interlocked.Increment(ref _subscriptions);
            var subscription = new Subscription(port, callback, _serialiser);
            subscription.Start();
            return subscription;
        }

        public void Publish(Message message)
        {
            var stream = new MemoryStream();
            _serialiser.Serialise(message, stream);
            var buffer = stream.ToArray();

            // Send message to each port
            for (var i = BasePort + 1; i <= BasePort  + _subscriptions; i++)
            {
                _udpPublishClient.SendAsync(buffer, buffer.Length, new IPEndPoint(IPAddress.Loopback, i));
            }
        }

        private class Subscription : IDisposable
        {
            private readonly Func<Message, Task<bool>> _callback;
            private readonly IMessageSerialiser _serialiser;
            private readonly UdpClient _udpClient;
            private bool _isRunning;

            public Subscription(int port, Func<Message, Task<bool>> callback, IMessageSerialiser serialiser)
            {
                _callback = callback;
                _serialiser = serialiser;
                _udpClient = new UdpClient(port);
            }

            public async void Start()
            {
                _isRunning = true;
                while (_isRunning)
                {
                    var result = await _udpClient.ReceiveAsync();
                    var stream = new MemoryStream(result.Buffer);
                    var message = _serialiser.Deserialise(stream);
                    _callback(message);
                }
            }

            public void Dispose()
            {
                _isRunning = false;
            }
        }
    }
}