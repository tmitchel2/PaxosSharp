using System;
using System.Diagnostics;
using PaxosSharp.MessageBus;
using PaxosSharp.Storage;

namespace PaxosSharp
{
    public sealed class PaxosConfiguration
    {
        public PaxosConfiguration()
        {
            MessageBus = new UdpSingleMachineMessageBus();
            NonVolatileStorage = new InMemoryNonVolatileStorage();
            TraceSource = new TraceSource("Paxos", SourceLevels.Verbose);
            
            // Capacities
            AcceptorCount = 3;
            ProposerCountMaxValue = 10;
            ProposerPreExecWinSize = 50;
            ProposerRecordCapacity = 512;
            ProposerPhase2Concurrency = 2000;
            LeaderProposerQueueCapacity = 50;

            // Timeouts
            Phase1Timeout = TimeSpan.FromMilliseconds(30);
            Phase2Timeout = TimeSpan.FromMilliseconds(150);

            // Check loop intervals
            Phase1TimeoutCheckInterval = TimeSpan.FromMilliseconds(100);
            Phase2TimeoutCheckInterval = TimeSpan.FromMilliseconds(100);
            FailureDetectionInterval = TimeSpan.FromSeconds(5);
            LearnerMissingMessageCheckInterval = TimeSpan.FromMilliseconds(100);
            LeaderEventsUpdateInterval = TimeSpan.FromSeconds(10);
            LeaderEventsIsEnabled = false;
        }

        public IMessageBus MessageBus { get; private set; }

        public INonVolatileStorage NonVolatileStorage { get; private set; }

        public TraceSource TraceSource { get; private set; }

        public bool LeaderEventsIsEnabled { get; private set; }

        public int AcceptorCount { get; private set; }

        public int ProposerCountMaxValue { get; private set; }

        public int ProposerPhase2Concurrency { get; private set; }

        public int ProposerPreExecWinSize { get; private set; }
        
        public int ProposerRecordCapacity { get; private set; }

        public int LeaderProposerQueueCapacity { get; private set; }

        public TimeSpan Phase1Timeout { get; private set; }

        public TimeSpan Phase2Timeout { get; private set; }

        public TimeSpan Phase1TimeoutCheckInterval { get; private set; }

        public TimeSpan Phase2TimeoutCheckInterval { get; private set; }

        public TimeSpan FailureDetectionInterval { get; private set; }

        public TimeSpan LearnerMissingMessageCheckInterval { get; private set; }
        
        public TimeSpan LeaderEventsUpdateInterval { get; private set; }
    }
}