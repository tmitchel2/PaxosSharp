using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// Repeat message.  Used to sync a learner who has fallen behind.
    /// </summary>
    internal class RepeatMessage : Message
    {
        public RepeatMessage(IEnumerable<int> instanceIds)
        {
            InstanceIds = new ReadOnlyCollection<int>(instanceIds.ToList());
        }

        public IList<int> InstanceIds { get; private set; }
    }
}