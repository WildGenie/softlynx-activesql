using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Diagnostics;
using System.Threading;
/*
namespace WrapperTest
{
    class CEWrapper
    {
        static void Main(string[] args)
        {
            Softlynx.XDelta3.Wrapper.Decode(@"\Temp\src",@"\Temp\patch",@"\Temp\dst.wce");
        }
    }
}
*/

namespace Softlynx.XDelta3
{
    public class Wrapper
    {
        static string RunXDelta(params string[] args)
        {
            string fn = Path.GetTempFileName();

            using (Stream exe = File.Open(fn, FileMode.Create, FileAccess.ReadWrite,  FileShare.ReadWrite))
            {
                byte[] execontent = Softlynx.XDelta3.Properties.Resources.XDelta3Decode;
                exe.Write(execontent, 0, execontent.Length);
                exe.Close();
            }
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.UseShellExecute = false;
            startInfo.FileName = fn;
            foreach (string a in args) 
            {
                startInfo.Arguments += string.Format("\"{0}\" ", a);
            }
            string res = null;
            int ExitCode=0;
            using (Process p = Process.Start(startInfo))
            {
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
                throw new System.ApplicationException("Error processing " + startInfo.Arguments);
            return res;
        }

        public static void Decode(string original, string patch, string output)
        {
            RunXDelta(original, patch, output);
        }

    }
}
