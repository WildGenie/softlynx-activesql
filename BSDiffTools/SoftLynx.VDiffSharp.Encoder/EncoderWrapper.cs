using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;

namespace SoftLynx.VDiffSharp
{
    public class VDiffEncoder
    {
        public static void BzipEncode(string OriginalFileName, string OutputFileName, string PatchFileName)
        {
            using (Stream fs = new ICSharpCode.SharpZipLib.BZip2.BZip2OutputStream(File.Open(PatchFileName, FileMode.Create, FileAccess.ReadWrite)))
            {
                VDiffEncoder.Encode(
                        OriginalFileName,
                        OutputFileName,
                        fs);
            };
        }

        public static void Encode(string original, string output, Stream patch)
        {
            string fn = Path.GetTempFileName();
            File.WriteAllBytes(fn, Softlynx.VDiffSharp.Encoder.Properties.Resources.xdelta3_0z_x86_32);

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
