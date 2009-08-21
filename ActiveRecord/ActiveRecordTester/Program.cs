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
 
    [InTable]
    [WithReplica]
    public class DemoProperty : ObjectProp { }


    [InTable]
    [WithReplica]
    [TableVersion(6,TableAction.None)]
    class DemoObject:DynamicObject<DemoProperty>
    {
        public DemoObject() : base() { }
        public DemoObject(RecordManager manager) : base(manager) { }
        public new class Property
        {
            static public PropType Name = new PropType<string>("Name","{A34E00AF-4A88-46e6-8DE6-539A119C3A22}");
            static public PropType LastName = new PropType<string>("Name", "{A34E00AF-4A88-46e6-8DE6-539A119C3A21}");
            static public PropType StatusFlag = new PropType<bool>("Status");
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

        public bool Checked
        {
            get { return GetValue<bool>(Property.StatusFlag,false); }
            set { SetValue<bool>(Property.StatusFlag, value); }
        }


        int c1=0;
        [OnTableVersionChange(PostRegistration=true)]
        static public void newVersion(int version)
        {
            version = 0;
        }
    }

    public class O1 : PropertySet
    {
        public class Property
        {
            static public PropType Name1 = new PropType<string>("Name1");
            static public PropType Name2 = new PropType<string>("Name2");
            static public PropType ID = new PropType<Guid>("ID");
        }

        public string Name1
        {
            get { return GetValue<string>(Property.Name1,new DefaultValueDelegate<string>( delegate {return Name2;})); }
            set { SetValue<string>(Property.Name1,value); }
        }

        public string Name2
        {
            get { return GetValue<string>(Property.Name2, new DefaultValueDelegate<string>(delegate { return ID.ToString(); })); }
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
        public new class Property
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

    [InTable(Name="ItemsDescr1")]
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
            
            string tttt=string.Format("",o1.Name1);
            PropType[] deps1 = o1.PropsDependsOn(O1.Property.ID);

            O2 o2 = new O2();
            tttt = string.Format("", o2.Name2);
            PropType[] deps2 = o2.PropsDependsOn(O2.Property.ID);
            o1.OnPropertyChanged += new PropertyValueChange(o1_OnPropertyValueChanged);
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
            
            Thread t = new Thread(new ThreadStart(RunTests));
            //Thread t = new Thread(new ThreadStart(s.Run));
            t.Name = "Test run";
            t.Start();
            /*
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
             */
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
            string rnd = "askljhqwpoeruiqwopeqwoiuäæëîéöóêçùøõ2çù3øçùøéöâôæûùçêã1834798àëäîôûðâà";
            Random r=new Random();
            while (p.Count < 100)
            {
                string key= "ëîð äùã " + Guid.NewGuid().ToString() + " ôûîàðä éùö3øã";
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

                //ProviderSpecifics prov = new OleDBSpecifics();
                //prov.ExtendConnectionString("provider", "Microsoft.Jet.OLEDB.4.0");
                //prov.ExtendConnectionString("data source", @"C:\Program Files\Starboard Inventory\SBDB.mdb");
                //prov.ExtendConnectionString("Jet OLEDB:Database Password", "sa23dk89");

                ProviderSpecifics prov = new SQLiteSpecifics();
                prov.ExtendConnectionString("Data Source", @"c:\tests.db3");
                prov.ExtendConnectionString("BinaryGUID","FALSE");

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
            public new class Property
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
        [InTable(Name = "Employees1")]
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

        //[InTable]
        public class tmpPPTNewDescrLog : IDObject
        {
            public new class Property
            {
                // idPPTNewDescr ???
                static public PropType idPPTNewDescr = new PropType<long>("idPPTNewDescr");
                static public PropType SKUCode = new PropType<long>("SKUCode");
                static public PropType SyncNo = new PropType<long>("SyncNo");
                static public PropType Barcode = new PropType<double>("Barcode");
                static public PropType SKUDesc = new PropType<string>("SKUDesc");
                static public PropType RetailPrice = new PropType<decimal>("RetailPrice");
                static public PropType TStamp = new PropType<DateTime>("TStamp");
                static public PropType flgModify = new PropType<int>("flgModify");
                static public PropType EmplID = new PropType<long>("EmplID");
                static public PropType flgUploaded = new PropType<long>("flgUploaded");
            }

        //[PrimaryKey(false)]
        new protected Guid ID { get { return Guid.Empty; } }
        
//        [PrimaryKey]
        [Autoincrement]
        public long idPPTNewDescr
        {
            get { return GetValue<long>(Property.idPPTNewDescr, 0); }
            set { SetValue<long>(Property.idPPTNewDescr, value); }
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

            [InField(Size = 8)]
            //[ExcludeFromTable]
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

            public long EmplID
            {
                get { return GetValue<long>(Property.EmplID, 0); }
                set { SetValue<long>(Property.EmplID, value); }
            }
        }

    static void RunTests()
    {
        /*
                ProviderSpecifics MDB_prov = new OleDBSpecifics();
                MDB_prov.AutoSchema = false;
                MDB_prov.ExtendConnectionString("provider", "Microsoft.Jet.OLEDB.4.0");
                MDB_prov.ExtendConnectionString("data source", @"C:\Program Files\Starboard Inventory\sbdb.mdb");
                MDB_prov.ExtendConnectionString("Jet OLEDB:Database Password", "sa23dk89");

                ProviderSpecifics SNAP_prov = new SQLiteSpecifics();
                string tmp = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".db3");
               
                SNAP_prov.ExtendConnectionString("Data Source", tmp);
                SNAP_prov.ExtendConnectionString("BinaryGUID","FALSE");

            
                    RecordManager MDB = new RecordManager(MDB_prov,
                        typeof(MDB_pptItemsDescr),
                        typeof(MDB_ClosedAreas),
                        typeof(MDB_tblSBParams),
                        typeof(MDB_Employees)
                        );
                    RecordManager SNAP = new RecordManager(SNAP_prov,
                        typeof(ItemsDescr),
                        typeof(ClosedAreas),
                        typeof(Employees),
                        typeof(tblSBParams)
                        );
                using (ManagerTransaction trans = SNAP.BeginTransaction())
                {
                    foreach (MDB_pptItemsDescr inv in RecordIterator.Enum<MDB_pptItemsDescr>(MDB))
                    {
                        ItemsDescr id = new ItemsDescr();
                        id.CopyFrom(inv);
                        id.ID = Guid.NewGuid();
                        SNAP.Write(id);
                    }

                    foreach (MDB_ClosedAreas inv in RecordIterator.Enum<MDB_ClosedAreas>(MDB))
                    {
                        ClosedAreas l_area = new ClosedAreas();
                        l_area.CopyFrom(inv);
                        l_area.ID = Guid.NewGuid();
                        SNAP.Write(l_area);
                    }

                    foreach (MDB_Employees inv in RecordIterator.Enum<MDB_Employees>(MDB))
                    {
                        Employees l_emp = new Employees();
                        l_emp.CopyFrom(inv);
                        l_emp.ID = Guid.NewGuid();
                        SNAP.Write(l_emp);
                    }

                    foreach (MDB_tblSBParams inv in RecordIterator.Enum<MDB_tblSBParams>(MDB))
                    {
                        tblSBParams l_par = new tblSBParams();
                        l_par.CopyFrom(inv);
                        l_par.ID = Guid.NewGuid();
                        SNAP.Write(l_par);
                    }

                    trans.Commit();
                }


                MDB.FlushConnectionPool();
                MDB.Connection.Close();
                MDB.Dispose();

                SNAP.FlushConnectionPool();
                SNAP.Connection.Close();
                SNAP.Dispose();
         */
        
                //RecordManager rm = RecordManager.Default;
                //RecordManager.Default = null;
                //RecordManager.Default = rm;
                DemoObject dom = new DemoObject(RecordManager.Default);
                bool isnew = dom.IsNewObject;
                //dom.ID = Guid.NewGuid();
                dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");
                string n = dom.Name;
                dom.Name = "name " + dom.ID.ToString()+Guid.NewGuid().GetHashCode().ToString();
                dom.LastName = "Last Name " + dom.ID.ToString() + Guid.NewGuid().GetHashCode().ToString();
                dom.Checked = true;
                //string ss2=r1.SerializeObject(dom);
                //RecordManager.Default.Read(dom);
                //RecordManager.Default.Write(dom);
                //RecordManager.Default.Write(dom);
                RecordManager.Default.Write(dom);
                dom = new DemoObject(RecordManager.Default);
                dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");
                RecordManager.Default.Read(dom);
                 isnew = dom.IsNewObject;
                bool br = dom.Checked;
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
