using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.IO;

namespace SvnRevision
{
    class Program
    {
        static int SourceRevision(string folder, out bool hasmodifications, string arguments)
        {
            hasmodifications = false;
            ProcessStartInfo pi = new ProcessStartInfo();
            pi.RedirectStandardOutput = true;
            pi.WorkingDirectory = folder;
            pi.FileName = "svnversion";
            pi.Arguments = arguments;
            pi.CreateNoWindow = true;
            pi.UseShellExecute = false;
            Process proc = System.Diagnostics.Process.Start(pi);
            int version = 0;
            while (!proc.StandardOutput.EndOfStream)
            {
                string[] vals = proc.StandardOutput.ReadLine().Split(':');
                string line = vals[vals.Length - 1];
                if (line.EndsWith("M", StringComparison.OrdinalIgnoreCase))
                {
                    hasmodifications = true;
                }
                int.TryParse(line.Trim('M'), out version);
            }
            proc.WaitForExit();
            if (version == 0)
            {
                throw new ApplicationException("Can't parse svnversion output");
            }
            if (hasmodifications) version++;
            return version;
        }

        static int SourceRevision(string folder, out bool hasmodifications)
        {
            return SourceRevision(folder, out hasmodifications, string.Empty);
        }

        static bool FixVersion(ref string value, int newversion)
        {
            bool modified = false;
            List<string> components = new List<string>(value.Split('.'));
            while (components.Count < 3) components.Add(string.Empty);
            if (components[2] != newversion.ToString())
            {
                components[2] = newversion.ToString();
                modified = true;
                value = string.Join(".", components.ToArray());
            };
            return modified;
        }

        static int Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: SvnRevision.exe <folder> [path to AssemblyInfo.cs or .vdproj]");
                return -1;
            }
            try
            {
                string folder = args[0];
                bool modified = false;
                int version = SourceRevision(folder, out modified, "-c");
                if (modified) version = SourceRevision(folder, out modified);
                Console.WriteLine(version.ToString());
                string subfile = Path.Combine(folder, args.Length > 1 ? args[1] : @"Properties\assemblyinfo.cs");
                string[] lines=File.ReadAllLines(subfile,Encoding.UTF8);
                List<string> outlines = new List<string>();
                Regex pattern = new Regex(@"(AssemblyVersion|AssemblyFileVersion)\(""(.*)""\)",
                    RegexOptions.Compiled);
                Regex prodcode = new Regex(@"(""ProductCode"" = ""8:)({.*\})("".*)",
    RegexOptions.Compiled);
                Regex productversion = new Regex(@"(""ProductVersion"" = ""8):(.*)("".*)",
RegexOptions.Compiled);

                string NewProdCodeLine = null;
                int UpgradeProdLineNumber = 0;
                bool ChangeProdCode = false;
                modified = false;
                foreach (string code in lines)
                {
                    string s=code;
                    Match m = pattern.Match(s);
                    if (m.Success)
                    {
                        string t = m.Groups[2].Value;
                        if (FixVersion(ref t, version))
                        {
                            modified = true;
                            s = m.Result("$`$1(\"" + t + "\")$'");
                        }
                    }
                    m = prodcode.Match(s);
                    if (m.Success)
                    {
                        NewProdCodeLine = m.Result("$`$1{"+ Guid.NewGuid().ToString().ToUpper()+"}$3$'");
                        UpgradeProdLineNumber = outlines.Count;
                    }
                    m = productversion.Match(s);
                    if (m.Success)
                    {
                        string t = m.Groups[2].Value;
                        if (FixVersion(ref t, version))
                           {
                           s = m.Result("$`$1:" + t + "$3$'");
                           modified = true;
                           ChangeProdCode = true;
                           }
                    }

                
                    outlines.Add(s);
                }
                if (ChangeProdCode && (NewProdCodeLine != string.Empty))
                    outlines[UpgradeProdLineNumber] = NewProdCodeLine;
                if (modified)
                    File.WriteAllLines(subfile, outlines.ToArray(), Encoding.UTF8);
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -2;
            }
        }
    }
}
