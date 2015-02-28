using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("Projection: {ProjectionName}")]
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

            return string.Format(ProjectionName + ": " + valueString);
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