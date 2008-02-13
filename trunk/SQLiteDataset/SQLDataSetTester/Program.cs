using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.Runtime.Serialization;
using System.IO;

namespace SQLDataSetTester
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
/*            Object[] arr = new Object[] { "dewe", 10, 2.3 };
            IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            MemoryStream strm = new MemoryStream();
            formatter.Serialize(strm, arr);
            strm.Flush();
            strm.Seek(0,SeekOrigin.Begin);
            Object[] arr1 = (Object[])formatter.Deserialize(strm);
            */
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new Form1());
        }
    }
}