using System;
using System.Collections;
using System.Collections.Generic;
using System.Windows.Forms;
using System.IO;
using Softlynx.ActiveSQL;
using Softlynx.ActiveSQL.Postgres;
using Softlynx.RecordSet;
using Softlynx.SimpleConfig;

using System.Data;
using System.Data.Common;

namespace SQLDataSetTester
{
    [InTable]
    class DemoObject: IActiveRecordWriter
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

        [AfterRecordManagerWrite]
        void AfterWrite()
        {
            int i = 1;
        }

        [AfterRecordManagerRead]
        void AfterRead()
        {
            int i = 1;
        }

        public bool ActiveRecordWrite(RecordManager manager)
        {
            return true;
        }


    }

    /*
    [InTable]
    [TableVersion(1, "select 1")]
    [TableVersion(2, TableAction.Recreate)]
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
//    [TableVersion(1,"select 1")]
    [TableVersion(1,TableAction.Recreate)]
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


        public Guid LocationID
        {
            get { return _location; }
            set { _location = value; }
        }


    }

    [InTable]
    public class ObjectProp
    {
        Guid _id = Guid.Empty;
        Guid _object_id = Guid.Empty;
        Guid _property_id = Guid.Empty;
        Guid _author_id = Guid.Empty;
        DateTime _created = DateTime.Now;
        Object _value = null;

        public ObjectProp()
        {
        }
                
        public ObjectProp(Guid objectID, Guid propertyID, object value)
        {
            ObjectID = objectID;
            PropertyID = propertyID;
            Value = value;
        }

        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }

        [Indexed]
        public Guid ObjectID
        {
            get { return _object_id; }
            set { _object_id = value; }
        }

        [Indexed]
        public Guid PropertyID
        {
            get { return _property_id; }
            set { _property_id = value; }
        }

        [Indexed]
        public DateTime Created
        {
            get { return _created; }
            set { _created = value; }
        }


        public Guid AuthorID
        {
            get { return _author_id; }
            set { _author_id = value; }
        }

        public Object Value
        {
            get { return _value; }
            set { _value = value; }
        }

        [ExcludeFromTable]
        public string AsString
        {
            get { return (string)_value; }
            set { _value = value; }

        }

        [ExcludeFromTable]
        public decimal AsDecimal
        {
            get { return (decimal)_value; }
            set { _value = value; }

        }

        [ExcludeFromTable]
        public int AsInt
        {
            get { return (int)_value; }
            set { _value = value; }
        }

        [ExcludeFromTable]
        public double AsDouble
        {
            get { return (double)_value; }
            set { _value = value; }
        }

        [ExcludeFromTable]
        public Guid AsGuid
        {
            get { return (Guid)_value; }
            set { _value = value; }
        }

    }

    [InTable]
    public class DynamicObject:IRecordSetItem
    {
        public static class Properties {
            public static Guid Name = new Guid("{BDC3547B-6C2C-45ea-84EA-0BE03CC4E24C}");
            }

        Guid _id = Guid.NewGuid();

        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }
        private Dictionary<Guid, ObjectProp> changed_values=new Dictionary<Guid,ObjectProp>();
        private Dictionary<Guid,ObjectProp> props_latest_value=new Dictionary<Guid,ObjectProp>();

        public void Assigned()
        {
        }

        public bool HasChanges
        {
            get { return changed_values.Values.Count > 0; }
        }

        public void OnWrite()
        {
            foreach (ObjectProp op in changed_values.Values)
            {
                RecordBase.Write(op);
            }
        }

        public RecordSet<ObjectProp> GetPropertyChangeHistory(Guid PropertyID)
        {
            RecordSet<ObjectProp> r = new RecordSet<ObjectProp>();
            if (PropertyID == Guid.Empty)
            {
                r.Fill("ObjectID=@ObjectID", "Created DESC",
                    "ObjectID", ID);
            }
            else
            {
                r.Fill("ObjectID=@ObjectID and PropertyID=@PropertyID", "Created DESC",
    "ObjectID", ID,
    "PropertyID", PropertyID);
            }
            return r;
        }

        public RecordSet<ObjectProp> GetPropertyChangeHistory()
        {
            return GetPropertyChangeHistory(Guid.Empty);
        }
        
        public ObjectProp GetPropertyWithLastValue(Guid PropertyID)
        {
            ObjectProp r = null;
            try {
            r=props_latest_value[PropertyID];
            }
            catch (KeyNotFoundException) {
                RecordSet<ObjectProp> v = new RecordSet<ObjectProp>();
                v.Fill("ObjectID=@ObjectID and PropertyID=@PropertyID", "Created DESC",1,
                    "ObjectID", ID,
                    "PropertyID", PropertyID);
               if (v.Count==0) r = new ObjectProp(ID, PropertyID, null);
               else r = (ObjectProp)v[0];
            }
            props_latest_value[PropertyID] = r;
            return r;
        }

        public bool SetPropertyLastValue(Guid PropertyID,object value)
        {
            bool changed = false;
            ObjectProp r = GetPropertyWithLastValue(PropertyID);
            if ( 
                ((r.Value!=null) && (value!=null) && (!r.Value.Equals(value)))
                ||
                ((r.Value == null) && (value != null))
                ||
                ((r.Value != null) && (value == null))
                )
            {
            r.Value = value;
            changed_values[PropertyID]=r;
            changed = true;
            }
            
            return changed;
        }

    }
     */

    

    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            SimpleConfig.FileName = @"c:\mycfg.xml";
            SimpleConfig.Pairs["drink"] = "1";
            SimpleConfig.Pairs["vehicle"] = "bike";
            SimpleConfig.Save();
            SimpleConfig.Load();

            IProviderSpecifics prov = new PgSqlSpecifics();
            prov.ExtendConnectionString("Database", "tests");
            prov.ExtendConnectionString("Host", "sql.vladimir.psb");
            prov.ExtendConnectionString("User Id", "reporter");
            prov.ExtendConnectionString("Password", "reporter");
            prov.Connection.Open();
            RecordManager.Default = new RecordManager(prov,typeof(Program).Assembly.GetTypes());

            DemoObject dom = new DemoObject();
            dom.ID = Guid.NewGuid();
            dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");
            dom.Name = "name " + dom.ID.ToString();
            RecordManager.Default.Read(dom);
            RecordManager.Default.Write(dom);
            RecordManager.Default.Read(dom);

            RecordSet<DemoObject> drs = new RecordSet<DemoObject>();
            drs.Fill();



            /*
            Session.AttachDatabase(@"c:\temp\ar.db3");

            DynamicObject o = new DynamicObject();
            o.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");
            string n=o.GetPropertyWithLastValue(DynamicObject.Properties.Name).AsString;
            o.SetPropertyLastValue(DynamicObject.Properties.Name, "Holder");
            n = o.GetPropertyWithLastValue(DynamicObject.Properties.Name).AsString;

            if (o.HasChanges)   RecordBase.Write(o);

            Session.Detach();
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new ActiveRecordTest());
             */

        }
    }
     
}
