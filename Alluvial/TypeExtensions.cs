using System;
using System.ComponentModel.Composition;
using System.Text.RegularExpressions;

namespace Alluvial
{
    internal static class TypeExtensions
    {
        public static string ReadableName(this Type type)
        {
            return new Regex(@"([0-9\w\+]+\.)|([0-9\w\+]+\+)([\(\)]*)")
                .Replace(AttributedModelServices.GetContractName(type), "$3");
        }
    }
}