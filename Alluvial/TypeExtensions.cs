using System;
using System.ComponentModel.Composition;
using System.Text.RegularExpressions;

namespace Alluvial
{
    internal static class TypeExtensions
    {
        private static readonly Regex nameParser = new Regex(@"([0-9\w\+]+\.)|([0-9\w\+]+\+)([\(\)]*)", RegexOptions.Compiled);

        public static string ReadableName(this Type type) =>
            nameParser.Replace(AttributedModelServices.GetContractName(type), "$3");
    }
}