using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("Projection: {ToString()}")]
    public class Projection<TValue>
    {
        private static readonly string typeName = string.Format("Projection<{0}>", typeof (TValue).Name);

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

            return string.Format(TypeName + ": " + valueString);
        }

        protected virtual string TypeName
        {
            get
            {
                return typeName;
            }
        }
    }
}