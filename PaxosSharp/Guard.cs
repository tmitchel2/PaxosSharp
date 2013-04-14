using System;

namespace PaxosSharp
{
    public sealed class Guard
    {
        public static void ArgumentNotNull(object arg, string argName)
        {
            if (arg == null)
            {
                throw new ArgumentNullException(argName);
            }
        }

        public static void ArgumentOutOfRange(int arg, int minValue, int maxValue, string argName)
        {
            if (arg < minValue || arg > maxValue)
            {
                throw new ArgumentOutOfRangeException(
                    argName,
                    arg,
                    string.Format("Value was {0} but it must be between {1} and {2}", arg, minValue, maxValue));
            }
        }
    }
}