using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Softlynx.ActiveSQL;
//using Softlynx.SQLiteDataset.Replication;
//using System.Data.SQLite;

namespace SQLDataSetTester
{
    public partial class DataSetAndReplicationTests : Form
    {
        //SQLiteReplicator repl1 = new SQLiteReplicator();
        //SQLiteReplicator repl2 = new SQLiteReplicator();
        
        // SQLiteDatasetWrapper wrapper = new SQLiteDatasetWrapper();
        // SQLiteConnection db = new SQLiteConnection();
        public DataSetAndReplicationTests()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            /*
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder();
            builder.Add("Data Source", @"c:\temp.db3");
            db.ConnectionString = builder.ConnectionString;
            db.Open();
            wrapper.AttachDataset(exampleDataSet1,db);
            wrapper.Fill(exampleDataSet1.DataTable1);
            exampleDataSet1.DataTable1.AddDataTable1Row(Guid.NewGuid(), "kjhkjhlk", DateTime.Now).Delete(); ;
            */
            
            //System.IO.File.Delete(SQLiteReplicator.ConnectionFileName(sqLiteConnection1));
            //sqLiteConnection1.Open();

            //sqLiteDatasetWrapper1.AttachDataset(exampleDataSet1,sqLiteConnection1);

            //sqLiteConnection2.Open();
            //sqLiteDatasetWrapper2.AttachDataset(exampleDataSet2, sqLiteConnection2);

            //sqLiteDatasetWrapper1.Active = true;

            
            //repl1.MasterDB = sqLiteConnection1;
            //repl1.CreateTableReplicaLogSchema("DataTable1");

            //repl2.MasterDB = sqLiteConnection2;
            //repl2.CreateTableReplicaLogSchema("DataTable1");

            //repl.Open();
            
        }

        private void dataGridView1_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            try
            {

                exampleDataSet1.DataTable1.Rows[e.RowIndex].SetColumnError(e.ColumnIndex, e.Exception.Message);
                exampleDataSet1.DataTable1.Rows[e.RowIndex].RowError = e.Exception.Message;
            }
            catch { };
            e.Cancel = true;
        }

        private void dataGridView1_RowValidated(object sender, DataGridViewCellEventArgs e)
        {
            try
            {
                exampleDataSet1.DataTable1.Rows[e.RowIndex].ClearErrors();
            }
            catch { };
        }

        private void toolStripButton1_Click(object sender, EventArgs e)
        {
            Int64 last_id = 0;
            //byte[] buffer=repl1.BuildReplicaBuffer(ref last_id);
            //repl2.ApplyReplicaBuffer(buffer);

            /*
            
            last_id = 0;
            buffer = repl2.BuildLocalReplicaBuffer(ref last_id);
            repl1.ApplyReplicaBuffer(buffer);
             */

            //sqLiteDatasetWrapper2.PopulateDataSet(exampleDataSet2);
            //repl1.CreateSnapshot(@"c:\snap.db3");

        }

        private void toolStripButton8_Click(object sender, EventArgs e)
        {
            Int64 last_id = 0;
            //byte[] buffer = repl2.BuildReplicaBuffer(ref last_id);
            //repl1.ApplyReplicaBuffer(buffer);

            //sqLiteDatasetWrapper1.PopulateDataSet(exampleDataSet1);

        }

    }
}
