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
        static int Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: SvnRevision.exe <folder> [path to AssemblyInfo.cs]");
                return -1;
            }
            try
            {
                ProcessStartInfo pi = new ProcessStartInfo();
                pi.RedirectStandardOutput = true;
                pi.WorkingDirectory = args[0];
                pi.FileName = "svnversion";
                pi.CreateNoWindow = true;
                pi.UseShellExecute = false;
                Process proc = System.Diagnostics.Process.Start(pi);
                int version = 0;
                int delta = 0;
                while (!proc.StandardOutput.EndOfStream)
                {
                    string[] vals = proc.StandardOutput.ReadLine().Split(':');
                    string line = vals[vals.Length - 1];
                    if (line.EndsWith("M", StringComparison.OrdinalIgnoreCase))
                        delta += 1;
                    int.TryParse(line.Trim('M'), out version);
                }
                proc.WaitForExit();
                if (version == 0)
                {
                    throw new ApplicationException("Can't parse svnversion output");
                }
                version += delta;
                Console.WriteLine(version.ToString());
                string subfile=Path.Combine(pi.WorkingDirectory, args.Length>1?args[1]:@"Properties\assemblyinfo.cs");
                string[] lines=File.ReadAllLines(subfile,Encoding.UTF8);
                List<string> outlines = new List<string>();
                bool modified = false;
                Regex pattern = new Regex(@"(AssemblyVersion|AssemblyFileVersion)\(""(.*)""\)",
                    RegexOptions.Compiled);
                foreach (string code in lines)
                {
                    string s=code;
                    Match m = pattern.Match(s);
                    if (m.Success)
                    {
                        List<string> components = new List<string>(m.Groups[2].Value.Split('.'));
                        while (components.Count < 4) components.Add(string.Empty);
                        if (components[2] != version.ToString())
                        {
                            components[2] = version.ToString();
                            modified = true;
                        };
                        s = m.Result("$`$1(\"" + string.Join(".", components.ToArray()) + "\")$'");
                    }
                    outlines.Add(s);
                }
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
