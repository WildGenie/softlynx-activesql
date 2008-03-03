using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;


namespace Softlynx.Logger
{
    public class FileLogger
    {
        string LogFileName = string.Empty;
        FileStream logger = null;
        Encoding logger_encoding = new UTF8Encoding();
        public bool Enabled = false;

        public void LogMessage(string s)
        {
            if (!Enabled) return;


            if (logger == null)
            {
                logger = new FileStream(LogFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite);

            }
                byte[] buf = logger_encoding.GetBytes(
                    String.Format("{0:yyyy-MM-dd HH:mm:ss} :: {1:x4} :: {2}\n", DateTime.Now, Thread.CurrentThread.ManagedThreadId, s)
                    );
#if !PocketPC
                logger.Lock(0, 1);
#endif
                logger.Write(buf, 0, buf.Length);
                logger.Flush();
#if !PocketPC
                logger.Unlock(0, 1);
#endif
            
        }

        public void LogMessage(string format, params Object[] args)
        {
            LogMessage(string.Format(format, args));
        }


        public FileLogger(string FileName)
        {

            LogFileName = FileName;
#if !PocketPC
            foreach (EncodingInfo e in Encoding.GetEncodings())
            {
                if (e.Name == "windows-1251") { logger_encoding = e.GetEncoding(); break; };
            };

#endif
        }

        ~FileLogger()
        {
            //logger.Flush();
        }
    }
}
