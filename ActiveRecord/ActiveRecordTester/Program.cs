using System;
using System.Collections;
using System.Collections.Generic;
using System.Windows.Forms;
using System.IO;
using Softlynx.ActiveSQL;
using Softlynx.ActiveSQL.Postgres;
using Softlynx.ActiveSQL.SQLite;
using Softlynx.ActiveSQL.Replication;

using Softlynx.RecordSet;
using Softlynx.SimpleConfig;

using System.Data;
using System.Data.Common;

namespace ActiveRecordTester
{
 
    [InTable]
    [WithReplica]
    public class DemoProperty : ObjectProp { }


    [InTable]
    [WithReplica]
    class DemoObject:DynamicObject<DemoProperty>
    {

        public DemoObject(RecordManager manager) : base(manager) { }
        public class Property
        {
            static public PropType Name = new PropType("Name", "{A34E00AF-4A88-46e6-8DE6-539A119C3A22}");
        }

        [ExcludeFromTable]
        public string Name
        {
            get { return (string)GetPropertyLastValue(Property.Name); }
            set { SetPropertyLastValue(Property.Name,value); }
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
            
            //prov = new SQLiteSpecifics();
            //prov.ExtendConnectionString("Data Source", @"c:\tests.db3");
            //prov.ExtendConnectionString("BinaryGUID","FALSE");
            
            prov.Connection.Open();
            RecordManager.Default = new RecordManager(prov, typeof(Program).Assembly.GetTypes());
            ReplicaManager r1 = new ReplicaManager();
            r1.RegisterWithRecordManager(RecordManager.Default);
            DemoObject dom = new DemoObject(RecordManager.Default);
            //dom.ID = Guid.NewGuid();
            dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");

            string n = dom.Name;
            dom.Name = "name " + dom.ID.ToString();
            //string ss2=r1.SerializeObject(dom);
            //RecordManager.Default.Read(dom);
            RecordManager.Default.Write(dom);
            RecordManager.Default.Write(dom);
            RecordManager.Default.Write(dom);
            RecordManager.Default.Delete(dom);

            //RecordManager.Default.Read(dom);
            RecordSet<DemoObject> drs = new RecordSet<DemoObject>();
            drs.Fill();
            drs.Clear();
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
            prov.Connection.Close();
        }
    }
     
}
