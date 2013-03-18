using System;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using PaxosSharp;

namespace PaxosSharpTests
{
    [TestFixture]
    public class PaxosTests
    {
        [Test]
        public void Test1()
        {
            var config = new PaxosConfiguration(new SimpleMessageBus());
            config.TraceSource.Listeners.Add(new ConsoleTraceListener());

            var learner0 = new Learner(config, 0);
            var acceptor0 = new Acceptor(config, 0);
            var proposer0 = new Proposer(config, learner0, 0);

            learner0.Start();
            acceptor0.Start();
            proposer0.Start();

            var learner1 = new Learner(config, 1);
            var acceptor1 = new Acceptor(config, 1);
            var proposer1 = new Proposer(config, learner1, 1);

            learner1.Start();
            acceptor1.Start();

            var learner2 = new Learner(config, 2);
            var acceptor2 = new Acceptor(config, 2);
            var proposer2 = new Proposer(config, learner2, 2);

            learner2.Start();
            acceptor2.Start();

            proposer0.SubmitValue("TEST 1");

            string value;
            if (!learner2.TryGetNextValue(out value))
            {
                throw new Exception("Expected a message");
            }

            Assert.AreEqual("TEST 1", value);
        }
    }
}
