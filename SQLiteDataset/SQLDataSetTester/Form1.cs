using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Softlynx.SQLiteDataset;
using System.Data.SQLite;

namespace SQLDataSetTester
{
    public partial class Form1 : Form
    {
        SQLiteDatasetWrapper wrapper = new SQLiteDatasetWrapper();
        SQLiteConnection db = new SQLiteConnection();
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            DbConnectionStringBuilder builder = new DbConnectionStringBuilder();
            builder.Add("Data Source", @"c:\temp.db3");
            db.ConnectionString = builder.ConnectionString;
            db.Open();

            wrapper.AttachDataset(exampleDataSet1,db);
            wrapper.Fill(exampleDataSet1.DataTable1);

            exampleDataSet1.DataTable1.AddDataTable1Row(Guid.NewGuid(), "kjhkjhlk", DateTime.Now).Delete(); ;

        }
    }
}