using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;

namespace Softlynx.XDelta3
{
    public class Wrapper
    {
        static string RunXDelta(params string[] args)
        {
            string fn = Path.GetTempFileName();
            using (Stream exe = File.Open(fn, FileMode.Create, FileAccess.ReadWrite, FileShare.Delete | FileShare.ReadWrite | FileShare.Inheritable))
            {
                exe.Write(Softlynx.VDiffSharp.Encoder.Properties.Resources.xdelta3_0z_x86_32, 0, Softlynx.VDiffSharp.Encoder.Properties.Resources.xdelta3_0z_x86_32.Length);
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


        public static void Encode(string original, string output, Stream patch)
        {
            string fn = Path.GetTempFileName();
            //File.WriteAllBytes(fn, Softlynx.VDiffSharp.Encoder.Properties.Resources.xdelta3_0z_x86_32);

            string retMessage = String.Empty;
            ProcessStartInfo startInfo = new ProcessStartInfo();

            startInfo.CreateNoWindow = true;
            startInfo.RedirectStandardOutput = true;
            startInfo.RedirectStandardInput = true;
            startInfo.UseShellExecute = false;
            startInfo.FileName = fn;
            startInfo.Arguments = string.Format("-s \"{0}\" \"{1}\"", original, output);

            Process p = Process.Start(startInfo);
            int count = 0;
            do
            {
                byte[] buf = new byte[4096];
                count=p.StandardOutput.BaseStream.Read(buf, 0, buf.Length);
                patch.Write(buf, 0, count);
            } while (count>0);

            p.WaitForExit();

            File.Delete(fn);
        }
    }
}
