using System;
using System.Collections;
using System.Collections.Generic;
using System.Windows.Forms;
using System.Net;
using System.IO;
using System.Text;
using Softlynx.ActiveSQL;
using Softlynx.ActiveSQL.Postgres;
using Softlynx.ActiveSQL.SQLite;
using Softlynx.ActiveSQL.OleDB;
using Softlynx.ActiveSQL.Replication;
using Softlynx.RecordSet;
using Softlynx.RecordCache;
using Softlynx.SimpleConfig;
using Softlynx.SimpleRemoting;
using System.Threading;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

namespace ActiveRecordTester
{
 
    //[InTable]
    //[WithReplica]
    public class DemoProperty : ObjectProp { }


    //[InTable]
    //[WithReplica]
    //[TableVersion(4,ColumnAction.Remove,"C1")]
    class DemoObject:DynamicObject<DemoProperty>
    {
        public DemoObject() : base() { }
        public DemoObject(RecordManager manager) : base(manager) { }
        public class Property
        {
            static public PropType Name = new PropType<string>("Name","{A34E00AF-4A88-46e6-8DE6-539A119C3A22}");
            static public PropType LastName = new PropType<string>("Name", "{A34E00AF-4A88-46e6-8DE6-539A119C3A21}");
        }

        [ExcludeFromTable]
        public string Name
        {
            get { return (string)GetPropertyLastValue(Property.Name); }
            set { SetPropertyLastValue(Property.Name,value); }
        }

        [ExcludeFromTable]
        public string LastName
        {
            get { return (string)GetPropertyLastValue(Property.LastName); }
            set { SetPropertyLastValue(Property.LastName, value); }
        }

        int c1=0;
    }

    public class O1 : PropertySet
    {
        public class Property
        {
            static public PropType Name1 = new PropType<string>();
            static public PropType Name2 = new PropType<string>();
            static public PropType ID = new PropType<Guid>();
        }

        public string Name1
        {
            get { return GetValue<string>(Property.Name1,string.Empty); }
            set { SetValue<string>(Property.Name1,value); }
        }

        public string Name2
        {
            get { return GetValue<string>(Property.Name2,string.Empty); }
            set { SetValue<string>(Property.Name2, value); }
        }

        public Guid ID
        {
            get { return GetValue<Guid>(Property.ID, Guid.NewGuid()); }
        }

    }

    public class O2 : O1
    {
    
    }
    
    [InTable]
    public class ItemsDescr : IDObject
    {
        public class Property
        {
            static public PropType IntID = new PropType<long>("long ID Object identifier");
            static public PropType SKUDesc = new PropType<string>("SKU Description");
            static public PropType SKUCode = new PropType<long>("SKU Code");
            static public PropType Barcode = new PropType<double>("Barcode");
            static public PropType Department = new PropType<string>("Department");
            static public PropType RetailPrice = new PropType<Money>("Retail Price");
        }

        public long SKUCode
        {
            get { return GetValue<long>(Property.SKUCode, 0); }
            set { SetValue<long>(Property.SKUCode, value); }
        }
        
        public string SKUDesc
        {
            get { return GetValue<string>(Property.SKUDesc, string.Empty); }
            set { SetValue<string>(Property.SKUDesc, value); }
        }

        public double Barcode
        {
            get { return GetValue<double>(Property.Barcode, 0.0); }
            set { SetValue<double>(Property.Barcode, value); }
        }

        public int Department
        {
            get { return GetValue<int>(Property.Department, 0); }
            set { SetValue<int>(Property.Department, value); }
        }

        [InField(DbType.Currency)]
        public decimal RetailPrice
        {
            get { return GetValue<decimal>(Property.RetailPrice, 0); }
            set { SetValue<decimal>(Property.RetailPrice, value); }
        }

    }

    [InTable(Name="ItemsDescr")]
    public class MDB_ItemsDescr : ItemsDescr
    {
        new protected Guid ID { get { return Guid.Empty; } }

        [PrimaryKey]
        new public long SKUCode
        {
            get { return base.SKUCode; }
            set { base.SKUCode=value; }
        }
        public DateTime DTM
        {
            get { return DateTime.Now; }
            set {  }
        }

    }
    
    static class Program
    {
        [ThreadStatic]
        static RecordManager RM = null;

        static CacheCollector cache = new CacheCollector(TimeSpan.FromMilliseconds(100));

        
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            O1 o1 = new O1();
            O2 o2 = new O2();
            o1.OnPropertyValueChanged += new PropertyValueChanged(o1_OnPropertyValueChanged);
            string ss = o1.Name1;
            //o1.Name1 = "123";
            //o1.Name2 = "345";
            //o2.Name1 = "123";
            //o2.Name2 = "345";
            if (o2.Equals(o1))
            {
                PropType[] cp1 = o1.ChangedProperties;
            }
            
            PropType[] cp = o1.ChangedProperties;

            //return;
        //    string xml=ValueFormatter<int>.Serialize(2345);
        //    int a = ValueFormatter<int>.Deserialize(xml);

            SimpleConfig.FileName = @"c:\mycfg.xml";
            SimpleConfig.Pairs["drink"] = "1";
            SimpleConfig.Pairs["vehicle"] = "bike";
            SimpleConfig.Save();
            SimpleConfig.Load();


            RecordManager.ProviderDelegate=new RecordManagerProvider(ProvideRecordManager);
            Server s = new Server(new IPEndPoint(IPAddress.Any, 9090), new MessageHandler(MyHandler));
            
            //Thread t = new Thread(new ThreadStart(RunTests));
            Thread t = new Thread(new ThreadStart(s.Run));
            t.Name = "Test run";
            t.Start();
            using (Client c = new Client(new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9090)))
            {
                RemotingParams p = new RemotingParams();
                FillParams(p.Input);
                c.Query(p);
                p.Input.Clear();
                try
                {
                    c.Query(p);
                }
                catch
                {
                }
                FillParams(p.Input);
                c.Query(p);

            }

            s.Terminate();
            t.Join();
            t = null;
            GC.WaitForPendingFinalizers();
            RM = null;

            RunTests();
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
            //prov.Connection.Close();
        }
        static void FillParams(IDictionary p)
        {
            string rnd = "askljhqwpoeruiqwopeqwoiu������������2��3��������������1834798���������";
            Random r=new Random();
            while (p.Count < 100)
            {
                string key= "��� ��� " + Guid.NewGuid().ToString() + " ������ ���3��";
                string value=string.Empty;
                while (value.Length < 1024*16)
                    value += rnd;
                p.Add(key, value);
            }
        }
        static public void MyHandler(RemotingParams parameters)
        {
            if (parameters.Phase == RemotingPhase.Established)
            {
                // initialize the session
                return;
            }
            if (parameters.Phase == RemotingPhase.Disposing)
            {
                // dispose the session
                return;
            }
            if (parameters.Phase != RemotingPhase.Query)
            {
                // should never happened
                throw new ApplicationException("Unknown remoting phase");
            }


            if (parameters.Input.Count == 0) 
                //throw new ApplicationException("wwqwer");
            FillParams(parameters.Output);
        }
        static RecordManager ProvideRecordManager()
        {
            if (RM==null) {
                //IProviderSpecifics prov = new PgSqlSpecifics();
                //prov.ExtendConnectionString("Database", "test");
                //prov.ExtendConnectionString("Host", "localhost");
                //prov.ExtendConnectionString("User Id", "test");
                //prov.ExtendConnectionString("Password", "test");

                ProviderSpecifics prov = new OleDBSpecifics();
                prov.ExtendConnectionString("provider", "Microsoft.Jet.OLEDB.4.0");
                prov.ExtendConnectionString("data source", @"C:\Program Files\Starboard Inventory\SBDB.mdb");
                prov.ExtendConnectionString("Jet OLEDB:Database Password", "sa23dk89");

                //prov = new SQLiteSpecifics();
                //prov.ExtendConnectionString("Data Source", @"c:\tests.db3");
                //prov.ExtendConnectionString("BinaryGUID","FALSE");

                //prov.Connection.Ev
                //prov.Connection.ConnectionString
                prov.Connection.Open();
                //DbCommand dbcm = prov.Connection.CreateCommand();
                //dbcm.CommandText = "INSERT INTO ItemsDescr(SCUDesc,RetailPrice) values(@V1,@v2)";
                //OleDbParameter db1=new OleDbParameter("V1",OleDbType.
                //dbcm.Parameters.Add(prov.C
                //dbcm.Prepare();
                
                RM = new RecordManager(prov, typeof(Program).Assembly.GetTypes());
                ReplicaManager r1 = new ReplicaManager();
                r1.RegisterWithRecordManager(RM);
                }
        return RM;
        }

        [InTable]
        public class Employees : IDObject
        {
            public class Property
            {
                static public PropType Login = new PropType<long>("Login");
                static public PropType EmployeeName = new PropType<string>("EmployeeName");
                static public PropType Comments = new PropType<string>("Comments");
                static public PropType Password = new PropType<string>("Password");
                static public PropType isActive = new PropType<bool>("isActive");
                static public PropType IsAdmin = new PropType<bool>("IsAdmin");
            }

            public long Login
            {
                get { return GetValue<long>(Property.Login, 0); }
                set { SetValue<long>(Property.Login, value); }
            }
            public string EmployeeName
            {
                get { return GetValue<string>(Property.EmployeeName, string.Empty); }
                set { SetValue<string>(Property.EmployeeName, value); }
            }

            public string Comments
            {
                get { return GetValue<string>(Property.Comments, string.Empty); }
                set { SetValue<string>(Property.Comments, value); }
            }

            public string Password
            {
                get { return GetValue<string>(Property.Password, string.Empty); }
                set { SetValue<string>(Property.Password, value); }
            }

            public bool isActive
            {
                get { return GetValue<bool>(Property.isActive, true); }
                set { SetValue<bool>(Property.isActive, value); }
            }

            public bool IsAdmin
            {
                get { return GetValue<bool>(Property.IsAdmin, false); }
                set { SetValue<bool>(Property.IsAdmin, value); }
            }
        }
        [InTable(Name = "Employees")]
        public class MDB_Employees : Employees
        {
            new protected Guid ID { get { return Guid.Empty; } }

            [PrimaryKey]
            new public long Login
            {
                get { return base.Login; }
                set { base.Login = value; }
            }

            [InField(Size=8)]
            public DateTime DTM
            {
                get { return DateTime.Now; }
                set { }
            }

        }

        [InTable]
        public class tmpPPTNewDescrLog : IDObject
        {
            public class Property
            {
                // idPPTNewDescr ???
                static public PropType SKUCode = new PropType<long>("SKUCode");
                static public PropType SyncNo = new PropType<long>("SyncNo");
                static public PropType Barcode = new PropType<double>("Barcode");
                static public PropType SKUDesc = new PropType<string>("SKUDesc");
                static public PropType RetailPrice = new PropType<decimal>("RetailPrice");
                static public PropType TStamp = new PropType<DateTime>("TStamp");
                static public PropType flgModify = new PropType<int>("flgModify");
                static public PropType EmplID = new PropType<long>("EmplID");
            }
            public long SKUCode
            {
                get { return GetValue<long>(Property.SKUCode, 0); }
                set { SetValue<long>(Property.SKUCode, value); }
            }

            public long SyncNo
            {
                get { return GetValue<long>(Property.SyncNo, 0); }
                set { SetValue<long>(Property.SyncNo, value); }
            }

            public double Barcode
            {
                get { return GetValue<double>(Property.Barcode, 0); }
                set { SetValue<double>(Property.Barcode, value); }
            }

            public string SKUDesc
            {
                get { return GetValue<string>(Property.SKUDesc, string.Empty); }
                set { SetValue<string>(Property.SKUDesc, value); }
            }

            [InField(DbType.Currency)]
            public decimal RetailPrice
            {
                get { return GetValue<decimal>(Property.RetailPrice, 0); }
                set { SetValue<decimal>(Property.RetailPrice, value); }
            }

            [InField(Size=8)]
            public DateTime TStamp
            {
                get { return GetValue<DateTime>(Property.TStamp, DateTime.MinValue); }
                set { SetValue<DateTime>(Property.TStamp, value); }
            }

            public int flgModify
            {
                get { return GetValue<int>(Property.flgModify, 0); }
                set { SetValue<int>(Property.flgModify, value); }
            }

            public int EmplID
            {
                get { return GetValue<int>(Property.EmplID, 0); }
                set { SetValue<int>(Property.EmplID, value); }
            }
        }

    static void RunTests()
    {
        ProviderSpecifics prov1 = new OleDBSpecifics();
        prov1.AutoSchema = false;
        prov1.ExtendConnectionString("provider", "Microsoft.Jet.OLEDB.4.0");
        prov1.ExtendConnectionString("data source", @"C:\Program Files\Starboard Inventory\SBDB.mdb");
        prov1.ExtendConnectionString("Jet OLEDB:Database Password", "sa23dk89");

        ProviderSpecifics prov2 = new SQLiteSpecifics();
        prov2.ExtendConnectionString("Data Source", @"c:\tests.db3");
        prov2.ExtendConnectionString("BinaryGUID","FALSE");

        RecordManager RM1 = new RecordManager(prov1, typeof(tmpPPTNewDescrLog));
            foreach (tmpPPTNewDescrLog inv in RecordIterator.Enum<tmpPPTNewDescrLog>(RM1))
            {
                RM1.Write(inv);
            }
        return;
                //RecordManager rm = RecordManager.Default;
                //RecordManager.Default = null;
                //RecordManager.Default = rm;
                DemoObject dom = new DemoObject(RecordManager.Default);
                //dom.ID = Guid.NewGuid();
                dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");
                string n = dom.Name;
                dom.Name = "name " + dom.ID.ToString()+Guid.NewGuid().GetHashCode().ToString();
                dom.LastName = "Last Name " + dom.ID.ToString() + Guid.NewGuid().GetHashCode().ToString();
                //string ss2=r1.SerializeObject(dom);
                //RecordManager.Default.Read(dom);
                //RecordManager.Default.Write(dom);
                //RecordManager.Default.Write(dom);
                RecordManager.Default.Write(dom);

                //string xml = ReplicaManager.SerializeObject(dom);

                dom = new DemoObject(RecordManager.Default);
                //dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7A}");
                dom.ID = Guid.NewGuid();
                RecordManager.Default.Write(dom);
                RecordManager.Default.Delete(dom);

        
        
                RecordSet<DemoObject> drs = new RecordSet<DemoObject>();
                ArrayList l = new ArrayList();
                foreach (DemoObject dobj in RecordIterator.Enum<DemoObject>())
                {
                    string s = dobj.Name;
                    foreach (DemoObject dobj1 in RecordIterator.Enum<DemoObject>())
                    {
                        string s1 = dobj1.Name;
                    }
                }

                RecordManager.Default.FlushConnectionPool();
                drs.Fill();
                drs.Clear();

            }


        static void cache_OnObjectPurge(object instance)
        {
            (instance as RecordManager).Dispose();
        }

        static void o1_OnPropertyValueChanged(PropType property, object Value)
        {
            object o = Value;
        }

    }
     
}
