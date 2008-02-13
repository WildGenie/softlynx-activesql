using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Softlynx.SQLiteDataset;
using Softlynx.SQLiteReplicator;
using System.Data.SQLite;

namespace SQLDataSetTester
{
    public partial class Form1 : Form
    {
        SQLiteReplicator repl = new SQLiteReplicator();
        
        // SQLiteDatasetWrapper wrapper = new SQLiteDatasetWrapper();
        // SQLiteConnection db = new SQLiteConnection();
        public Form1()
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
            sqLiteConnection1.Open();
            sqLiteDatasetWrapper1.AttachDataset(exampleDataSet1,sqLiteConnection1);
            //sqLiteDatasetWrapper1.Active = true;

            
            repl.MasterDB = sqLiteConnection1;
            repl.CreateTableReplicaLogSchema("DataTable1");
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
            ReplicaPortion rp = new ReplicaPortion();
            rp.RequestLog(repl, 0);
        }
    }
}