using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.IO;
using Softlynx.SQLiteDataset.ActiveRecord;

namespace SQLDataSetTester
{
    [InTable, WithReplica]
    [TableVersion(1, "select 1")]
    class Location:IComparable
    {
        Guid _id = Guid.Empty;
        private string _Name;

        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }

        public string Name
        {
            get { return _Name; }
            set { _Name = value; }
        }

        public Location(){}

        public Location(string name)
        {
            ID = Guid.NewGuid();
            Name = name;
        }

        int IComparable.CompareTo(object l)    
        {
            return Name.CompareTo(((Location)l).Name);
        }


    }


    [InTable,WithReplica]
    [TableVersion(1,"select 1")]
    class Asset
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

        private Guid _location;

        [ForeignKey(typeof(Location))]
        public Guid LocationID
        {
            get { return _location; }
            set { _location = value; }
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
            
            RecordSet<Location> locs=new RecordSet<Location>();
            RecordSet<Asset> assets = new RecordSet<Asset>();
           
            Location loc = locs.Add(new Location("Location 1"));
            //RecordBase.Write(loc);
            
            loc = locs.Add(new Location("Location 2"));
            //RecordBase.Write(loc);
            
            locs.Fill();
            locs.Sort();

            Asset ast = new Asset();
            ast.LocationID = locs[0].ID;
            
            loc = locs[locs[2].ID];

            Session.Detach();
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new ActiveRecordTest());

        }
    }
}
