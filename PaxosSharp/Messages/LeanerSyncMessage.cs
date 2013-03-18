using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace PaxosSharp.Messages
{
    internal class LeanerSyncMessage : Message
    {
        public LeanerSyncMessage(IEnumerable<int> instanceIds)
        {
            InstanceIds = new ReadOnlyCollection<int>(instanceIds.ToList());
        }

        public IList<int> InstanceIds { get; private set; }
    }
}