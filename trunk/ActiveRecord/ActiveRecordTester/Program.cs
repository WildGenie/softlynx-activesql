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
using Softlynx.ActiveSQL.Replication;
using Softlynx.RecordSet;
using Softlynx.RecordCache;
using Softlynx.SimpleConfig;
using Softlynx.SimpleRemoting;
using System.Threading;
using System.Data.Common;

namespace ActiveRecordTester
{
 
    [InTable]
    [WithReplica]
    public class DemoProperty : ObjectProp { }


    [InTable]
    [WithReplica]
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
            t.Join();
            t = null;
            GC.WaitForPendingFinalizers();
            RM = null;
            
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
        static public void MyHandler(RemotingParams parameters)
        {
        }
        static RecordManager ProvideRecordManager()
        {
            if (RM==null) {
                IProviderSpecifics prov = new PgSqlSpecifics();
                prov.ExtendConnectionString("Database", "test");
                prov.ExtendConnectionString("Host", "localhost");
                prov.ExtendConnectionString("User Id", "test");
                prov.ExtendConnectionString("Password", "test");

                //prov = new SQLiteSpecifics();
                //prov.ExtendConnectionString("Data Source", @"c:\tests.db3");
                //prov.ExtendConnectionString("BinaryGUID","FALSE");

                //prov.Connection.Ev
                //prov.Connection.ConnectionString
                prov.Connection.Open();
                RM = new RecordManager(prov, typeof(Program).Assembly.GetTypes());
                ReplicaManager r1 = new ReplicaManager();
                r1.RegisterWithRecordManager(RM);
                }
        return RM;
        }

    static void RunTests()
    {

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
