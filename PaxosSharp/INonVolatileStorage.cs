using PaxosSharp.Messages;

namespace PaxosSharp
{
    public interface INonVolatileStorage
    {
        Acceptor.AcceptorState SavePrepare(PrepareRequestMessage message, Acceptor.AcceptorState state);

        Acceptor.AcceptorState SaveAccept(AcceptRequestMessage message);

        Acceptor.AcceptorState LoadAccept(int instanceId);
    }
}