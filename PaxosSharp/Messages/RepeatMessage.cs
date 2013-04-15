using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace PaxosSharp.Messages
{
    /// <summary>
    /// Repeat message.  Used to sync a learner who has fallen behind.
    /// </summary>
    [DataContract]
    internal class RepeatMessage : Message
    {
        public RepeatMessage(IEnumerable<int> instanceIds)
        {
            InstanceIds = new ReadOnlyCollection<int>(instanceIds.ToList());
        }

        [DataMember(Order = 1)]
        public IList<int> InstanceIds { get; private set; }
    }
}