using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;

namespace WrapperTest
{
    class CEWrapper
    {
        static void Main(string[] args)
        {
            Softlynx.XDelta3.Wrapper.Encode(
                        @"C:\temp\test\src",
                        @"C:\temp\test\dst",
                        @"C:\temp\test\patch");

            Softlynx.XDelta3.Wrapper.Decode(
            @"C:\temp\test\src",
            @"C:\temp\test\patch",
            @"C:\temp\test\dst.new"
            );

        }
    }
}

namespace Softlynx.XDelta3
{
    public class Wrapper
    {
        static string RunXDelta(params string[] args)
        {
            string fn = Path.GetTempFileName();
            using (Stream exe = File.Open(fn, FileMode.Create, FileAccess.ReadWrite, FileShare.Delete | FileShare.ReadWrite | FileShare.Inheritable))
            {
                byte[] execontent = Softlynx.XDelta3.Properties.Resources.XDelta3;
                exe.Write(execontent, 0, execontent.Length);
                exe.Close();
            }
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.CreateNoWindow = true;
            
            startInfo.RedirectStandardOutput = true;
            startInfo.RedirectStandardError = true;
            startInfo.RedirectStandardInput = true;
            startInfo.UseShellExecute = false;
            startInfo.FileName = fn;
            startInfo.ErrorDialog = false;

            foreach (string a in args) 
            {
                startInfo.Arguments += string.Format("\"{0}\" ", a);
            }
            string res = null;
            int ExitCode=0;
            using (Process p = Process.Start(startInfo))
            {
                res = p.StandardError.ReadToEnd()+p.StandardOutput.ReadToEnd();
                p.WaitForExit();
                ExitCode=p.ExitCode;
                p.Close();
            }
            while (true)
            {
                try
                {
                    File.Delete(fn);
                    break;
                }
                catch
                {
                    Thread.Sleep(100);
                }
            }
            if (ExitCode != 0)
                throw new System.ComponentModel.Win32Exception(res);

            return res;
        }

        public static void Encode(string original, string output, string patch)
        {
            RunXDelta("-v", "-v", "-f", "-9","-S","djw", "-e", "-s", original, output, patch);
        }

        public static void Decode(string original, string patch, string output)
        {
            RunXDelta("-v", "-v", "-f", "-d", "-s", original, patch, output);
        }
    }
}
