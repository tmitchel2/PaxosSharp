using System;
using System.Diagnostics;

namespace PaxosSharp
{
    public sealed class PaxosConfiguration
    {
        public PaxosConfiguration(IMessageBus messageBus)
        {
            MessageBus = messageBus;
            TraceSource = new TraceSource("Paxos", SourceLevels.Verbose);
            AcceptorCount = 3;
            ProposerCountMaxValue = 10;
            LearnerSyncInterval = TimeSpan.FromMilliseconds(100);
            PromiseTimeout = TimeSpan.FromMilliseconds(100);
            AcceptTimeout = TimeSpan.FromMilliseconds(100);
            ProposerPreExecWinSize = 50;
            AcceptorRecordCapacity = 1024;
            LearnerRecordCapacity = 512;
            ProposerRecordCapacity = 512;
            Phase1Timeout = TimeSpan.FromMilliseconds(30);
            Phase2Timeout = TimeSpan.FromMilliseconds(150);
            ProposerPhase2Concurrency = 2000;
        }

        public int ProposerPhase2Concurrency { get; private set; }

        public TimeSpan Phase1Timeout { get; private set; }

        public TimeSpan Phase2Timeout { get; private set; }

        public int AcceptorCount { get; private set; }

        public int ProposerCountMaxValue { get; private set; }

        public TimeSpan LearnerSyncInterval { get; private set; }

        public TimeSpan PromiseTimeout { get; private set; }

        public TimeSpan AcceptTimeout { get; private set; }

        public int ProposerPreExecWinSize { get; private set; }
        
        public int AcceptorRecordCapacity { get; private set; }

        public int LearnerRecordCapacity { get; private set; }

        public int ProposerRecordCapacity { get; private set; }

        public IMessageBus MessageBus { get; private set; }

        public TraceSource TraceSource { get; private set; }

        public bool LeaderEventsUpdateInterval { get; private set; }
    }
}