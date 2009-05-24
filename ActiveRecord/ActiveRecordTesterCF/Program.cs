using System;
using System.Net;
using System.Collections.Generic;
using System.Windows.Forms;
using Softlynx.ActiveSQL;
using Softlynx.ActiveSQL.SQLite;
using Softlynx.RecordSet;
using Softlynx.SimpleConfig;
using Softlynx.SimpleRemoting;


namespace ActiveRecordTesterCE
{
    [InTable]
    public class DemoProperty : ObjectProp { }

    [InTable]
    public class DemoObject : DynamicObject<DemoProperty>
    {
        public DemoObject() : base() { }
        public DemoObject(RecordManager manager) : base(manager) { }
        public class Property
        {
            static public PropType Name = new PropType<string>("Name","{A34E00AF-4A88-46e6-8DE6-539A119C3A22}");
        }

        public string Name
        {
            get { return (string)GetPropertyLastValue(Property.Name); }
            set { SetPropertyLastValue(Property.Name, value); }
        }
    }

    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [MTAThread]
        static void Main()
        {
            using (Client c = new Client(new IPEndPoint(IPAddress.Parse("192.168.1.10"), 9090)))
            {
                RemotingParams p = new RemotingParams();
                //FillParams(p.Input);
                c.Query(p);
                p.Input.Clear();
                try
                {
                    c.Query(p);
                }
                catch
                {
                }
                //FillParams(p.Input);
                c.Query(p);

            }

            SimpleConfig.FileName = @"\mycfg.xml";
            SimpleConfig.Pairs["drink"] = "1";
            SimpleConfig.Pairs["vehicle"] = "bike";
            SimpleConfig.Save();
            SimpleConfig.Load();

            ProviderSpecifics prov =  new SQLiteSpecifics();
            prov.ExtendConnectionString("Data Source", @"\tests.db3");
            prov.ExtendConnectionString("BinaryGUID", "FALSE");
            prov.Connection.Open();
            RecordManager.Default = new RecordManager(prov, typeof(Program).Assembly.GetTypes());

            DemoObject dom = new DemoObject(RecordManager.Default);
            //dom.ID = Guid.NewGuid();
            dom.ID = new Guid("{97C8BE02-1072-4797-8A37-E5D844272C7B}");

            string n = dom.Name;
            dom.Name = "name " + dom.ID.ToString();
            //              RecordManager.Default.Read(dom);
            RecordManager.Default.Write(dom);
            //RecordManager.Default.Read(dom);
            RecordSet<DemoObject> drs = new RecordSet<DemoObject>();
            drs.Fill();
            drs.Clear();
            prov.Connection.Close();
        }
    }
}