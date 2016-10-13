using System;
using System.Collections.Generic;

namespace Alluvial
{
    internal static class ExceptionExtensions
    {
        private static readonly HashSet<Type> transientExceptionTypes = new HashSet<Type>
        {
            typeof(TimeoutException)
        };

        public static bool IsTransient(this Exception exception) =>
            transientExceptionTypes.Contains(exception.GetType());

        internal static void MarkAsPublished(this Exception exception) =>
            exception.Data["__Alluvial__Published__"] = true;

        public static bool HasBeenPublished(this Exception exception) =>
            exception.Data.Contains("__Alluvial__Published__");
    }
}