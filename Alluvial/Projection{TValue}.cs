using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("ToString()")]
    public class Projection<TValue>
    {
        private static readonly string projectionName = typeof (Projection<TValue>).ReadableName();

        public TValue Value { get; set; }

        public override string ToString()
        {
            string valueString;

            var v = Value;
            if (v != null)
            {
                valueString = v.ToString();
            }
            else
            {
                valueString = "null";
            }

            return string.Format("{0}: {1}", ProjectionName, valueString);
        }

        protected virtual string ProjectionName
        {
            get
            {
                return projectionName;
            }
        }
    }
}