using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.IO;
using Softlynx.SQLiteDataset.ActiveRecord;

namespace SQLDataSetTester
{
    [InTable,WithReplica]
    [TableVersion(1,"select 1")]
    [TableVersion(2,"select 2")]
    class MyRecord
    {
        Guid _id = Guid.Empty;
        private string _Description;

        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }


        public string Description
        {
            get { return _Description; }
            set { _Description = value; }
        }



    }

    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            Session.AttachDatabase(@"c:\temp\ar.db3");
            MyRecord mr = new MyRecord();
            mr.ID = Guid.NewGuid();
            mr.Description = mr.ID.ToString().GetHashCode().ToString();
            RecordBase.Write(mr);
            mr.Description = mr.Description.GetHashCode().ToString();
            RecordBase.Write(mr);

            MyRecord[] records = (MyRecord[])RecordBase.Read(typeof(MyRecord));
            RecordBase.Delete(records[0]);

            Session.Detach();
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new ActiveRecordTest());

        }
    }
}
