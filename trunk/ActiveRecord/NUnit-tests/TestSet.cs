using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using Softlynx.ActiveSQL.Replication;
using Softlynx.ActiveSQL.SQLite;
using Softlynx.ActiveSQL.Postgres;
using Softlynx.ActiveSQL.MSSQL;
using NUnit.Framework;
using System.Reflection;

namespace NUnit_tests
{
    namespace Models
    
   {

        [InTable]
        [WithReplica]
        public class AutoIncrementObj
        {

            private Guid _ID = Guid.NewGuid();

            [PrimaryKey(false)]
            [Indexed]
            public Guid ID
            {
                get { return _ID; }
                set { _ID = value; }
            }

            Int64 n = 0;

            [Autoincrement]
            public Int64 AutoInc64
            {
                get { return n; }
                set { n=value; }
            }

        }


        [InTable]
        [WithReplica]
        public class BasicMapping : IDObject
        {
            public class Prop {
                static public PropType Text=new PropType<string>("Text field");
                static public PropType LongText = new PropType<string>("Long text field");
                static public PropType TimeStamp = new PropType<DateTime>("DateTime field");
                static public PropType TimePiece = new PropType<TimeSpan>("TimeSpan field");
                static public PropType Symbol = new PropType<char>("char field");
                static public PropType NumberByte = new PropType<byte>("byte field");
                static public PropType NumberShort = new PropType<short>("short field");
                static public PropType Number16 = new PropType<Int16>("INT16 field");
                static public PropType Number32 = new PropType<Int32>("INT32 field");
                static public PropType Number64 = new PropType<Int64>("INT64 field");
                static public PropType NumberU16 = new PropType<Int16>("UINT16 field");
                static public PropType NumberU32 = new PropType<Int32>("UINT32 field");
                static public PropType NumberU64 = new PropType<Int64>("UINT64 field");
                static public PropType NumberMoney = new PropType<Decimal>("Decimal field");
                static public PropType NumberSingle = new PropType<Single>("Single precision number");
                static public PropType NumberDouble = new PropType<Double>("Double precision number");
                static public PropType Checkbox = new PropType<bool>("flag value");
                static public PropType State = new PropType<FileAccess>("Enumerable field");
                static public PropType BLOB = new PropType<byte[]>("BLOB object");
            }

            public string Text {
                get {return GetValue<string>(Prop.Text,string.Empty);}
                set { SetValue<string>(Prop.Text, value); }
            }

            public string LongText
            {
                get { return GetValue<string>(Prop.LongText, string.Empty); }
                set { SetValue<string>(Prop.LongText, value); }
            }

            public DateTime TimeStamp
            {
                get { return GetValue<DateTime>(Prop.TimeStamp, DateTime.MinValue); }
                set { SetValue<DateTime>(Prop.TimeStamp, value); }
            }

            public char Symbol
            {
                get { return GetValue<char>(Prop.Symbol, char.MinValue); }
                set { SetValue<char>(Prop.Symbol, value); }
            }

            public byte NumberByte
            {
                get { return GetValue<byte>(Prop.NumberByte, byte.MinValue); }
                set { SetValue<byte>(Prop.NumberByte, value); }
            }
            
            public short NumberShort
            {
                get { return GetValue<short>(Prop.NumberShort, short.MinValue); }
                set { SetValue<short>(Prop.NumberShort, value); }
            }
            
            public Int16 Number16
            {
                get { return GetValue<Int16>(Prop.Number16, Int16.MinValue); }
                set { SetValue<Int16>(Prop.Number16, value); }
            }
            
            public Int32 Number32
            {
                get { return GetValue<Int32>(Prop.Number32, Int32.MinValue); }
                set { SetValue<Int32>(Prop.Number32, value); }
            }
            
            public Int64 Number64
            {
                get { return GetValue<Int64>(Prop.Number64, Int64.MinValue); }
                set { SetValue<Int64>(Prop.Number64, value); }
            }

            [RecordManagerPostRegistration]
            public static void CheckPostAction(RecordManager manager)
            {
                /*
                BasicMapping bm = new BasicMapping();
                bm.ID = Guid.NewGuid();
                manager.Read(bm);
                foreach (BasicMapping bm1 in RecordIterator.Enum<BasicMapping>())
                {
                    break;
                }
                 */ 
            }

            /*

            public UInt16 NumberU16
            {
                get { return GetValue<UInt16>(Prop.NumberU16, UInt16.MinValue); }
                set { SetValue<UInt16>(Prop.NumberU16, value); }
            }

            public UInt32 NumberU32
            {
                get { return GetValue<UInt32>(Prop.NumberU32, UInt32.MinValue); }
                set { SetValue<UInt32>(Prop.NumberU32, value); }
            }

            public UInt64 NumberU64
            {
                get { return GetValue<UInt64>(Prop.NumberU64, UInt64.MinValue); }
                set { SetValue<UInt64>(Prop.NumberU64, value); }
            }

            public TimeSpan TimePiece
            {  
                get { return GetValue<TimeSpan>(Prop.TimePiece, TimeSpan.MinValue); }
                set { SetValue<TimeSpan>(Prop.TimePiece, value); }
            }
              */


            public Decimal NumberMoney
            {
                get { return GetValue<Decimal>(Prop.NumberMoney, Decimal.Zero); }
                set { SetValue<Decimal>(Prop.NumberMoney, value); }
            }
            
            public Single NumberSingle
            {
                get { return GetValue<Single>(Prop.NumberSingle, Single.NaN); }
                set { SetValue<Single>(Prop.NumberSingle, value); }
            }

            public Double NumberDouble
            {
                get { return GetValue<Double>(Prop.NumberDouble, Double.NaN); }
                set { SetValue<Double>(Prop.NumberDouble, value); }
            }
            
            public bool Checkbox
            {
                get { return GetValue<bool>(Prop.Checkbox, false); }
                set { SetValue<bool>(Prop.Checkbox, value); }
            }
            
            public FileAccess State
            {
                get { return GetValue<FileAccess>(Prop.State, FileAccess.Read); }
                set { SetValue<FileAccess>(Prop.State, value); }
            }
            
            public byte[] BLOB
            {
                get { return GetValue<byte[]>(Prop.BLOB, new byte[]{}); }
                set { SetValue<byte[]>(Prop.BLOB, value); }
            }

            static internal BasicMapping _default=null;

            public static BasicMapping Default
            {
                get
                {
                    if (_default == null)
                        _default = RandomValue;
                    return _default;
                }
            }


            public static BasicMapping RandomValue
            {
                get {
                        Random r=new Random();
                        BasicMapping _v = new BasicMapping();
                        _v.ID = Guid.NewGuid();

                        byte[] tbuf = new byte[4096];
                        r.NextBytes(tbuf);
                        _v.LongText = Convert.ToBase64String(tbuf);

                        _v.Text = "Sample Text Value " + _v.ID.ToString() + " просто текст";
                        _v.Symbol = _v.Text[r.Next(0, _v.Text.Length - 1)];
                        //_v.TimeStamp = DateTime.Today.AddMilliseconds((double)decimal.Round((decimal)DateTime.Now.TimeOfDay.TotalMilliseconds,0));
                        _v.TimeStamp = DateTime.Today.AddSeconds((double)decimal.Round((decimal)DateTime.Now.TimeOfDay.TotalSeconds,0));
                        
                        
                        //_v.TimeStamp.Millisecond = _v.TimeStamp.Millisecond % 100;
                        _v.NumberByte = (byte)r.Next(byte.MinValue,byte.MaxValue);
                        _v.NumberShort = (short)r.Next(short.MinValue, short.MaxValue);

                        _v.Number16 = (Int16)r.Next(Int16.MinValue, Int16.MaxValue);
                        _v.Number32 = (Int32)r.Next(Int32.MinValue, Int32.MaxValue);
                        _v.Number64 = (Int64)r.Next(Int32.MinValue, Int32.MaxValue);


                        _v.NumberMoney =decimal.Round((r.Next(1, Int16.MaxValue) + 1m) / (r.Next(1, Int16.MaxValue) + 1m),14);
                        _v.NumberSingle = (float)decimal.Round( (decimal)r.NextDouble(),6);
                        _v.NumberDouble = (double)decimal.Round((decimal)r.NextDouble(), 14);
                        _v.State = FileAccess.Write;
                        _v.Checkbox = true;
                        byte[] buf=new byte[40];
                        r.NextBytes(buf);
                        _v.BLOB = buf;
                        return _v;
                    }
            }
        }
    }
    namespace Backends
    {

        [TestFixture("SQLITE", @"Data Source = c:\temp\test.db3;BinaryGUID=FALSE;")]
        [TestFixture("PGSQL", "host=localhost;Database=test;User Id=test;Password=test")]
        [TestFixture("MSSQL", @"Server=localhost\SQLEXPRESS;Database=test;User Id=test;Password=test;Trusted_Connection=false")]
        public class Backend
        {
            protected RecordManager RM = null;
            protected ProviderSpecifics prov = null;
            long EnumCount = 0;

            public Backend(string ProviderName, string ConnectionString)
            {
                Models.BasicMapping._default = null;
                if (ProviderName == "SQLITE")
                    prov = new SQLiteSpecifics();

                if (ProviderName == "PGSQL")
                    prov = new PgSqlSpecifics();

                if (ProviderName == "MSSQL")
                    prov = new MSSqlSpecifics();
                Assert.NotNull(prov);

                prov.Connection.ConnectionString = ConnectionString;
            }

            [Test(Description="Rest database to empty initial state"),Explicit]
            public void T00_EmptyDB()
            {
                if (prov is SQLiteSpecifics)
                {
                    string fn=Regex.Match(prov.Connection.ConnectionString,@"Data\s+Source\s*=\s*([^;]*)",RegexOptions.IgnoreCase).Groups[1].Value;
                    File.Delete(fn);
                }

                if (prov is PgSqlSpecifics)
                {
                    prov.Connection.Open();
                    DbCommand cmd = prov.Connection.CreateCommand();
                    cmd.CommandText="DROP SCHEMA public CASCADE;";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "CREATE SCHEMA public;";
                    cmd.ExecuteNonQuery();
                    prov.Connection.Close();
                }

                if (prov is MSSqlSpecifics)
                {
                    prov.Connection.Open();
                    DbCommand cmd = prov.Connection.CreateCommand();
                    cmd.CommandText = "exec sp_MSforeachtable \"DROP TABLE ? \";";
                    cmd.ExecuteNonQuery();
                    prov.Connection.Close();
                }
            }

            [Test(Description="Connects to database backend")]
            public void T01_ConnectDB()
            {
                    RM = new RecordManager(prov, new Type[] { typeof(Models.BasicMapping),typeof(Models.AutoIncrementObj) });
                    Assert.NotNull(RM);
                    Assert.IsTrue(RM.Connection.State == ConnectionState.Open);
            }


            [Test(Description = "Write heap of objects to database")]
            public void T02_WriteObjects()
            {
                object o = RM.RunScalarCommand("select count(*) from " + RM.AsFieldName("BasicMapping"));
                long pre_count = o==null?0:Convert.ToInt64(o);
                using (ManagerTransaction trans = RM.BeginTransaction())
                {
                    int i = 0;
                    DateTime start = DateTime.Now;
                    while (i++ < 100)
                    {
                        RM.Write(Models.BasicMapping.RandomValue);
                        RM.Write(Models.BasicMapping.Default);
                    }
                    trans.Commit();
                }
                o = RM.RunScalarCommand("select count(*) from " + RM.AsFieldName("BasicMapping"));
                long post_count = o == null ? 0 : Convert.ToInt64(o);
                Assert.AreEqual(post_count-pre_count,101);
            }

            [Test(Description = "Test transaction rollback properly")]
            public void T03_TransactionRollBack()
            {
                Models.BasicMapping tobj = Models.BasicMapping.RandomValue;
                using (ManagerTransaction trans = RM.BeginTransaction())
                {
                    RM.Write(tobj);
                    trans.Rollback();
                }
                Models.BasicMapping robj = new Models.BasicMapping();
                robj.ID = tobj.ID;
                Assert.IsTrue(!RM.Read(robj));
            }

            [Test(Description = "Read object back from DB")]
            public void T04_ReadObject()
            {
                Models.BasicMapping obj = new NUnit_tests.Models.BasicMapping();
                obj.ID = Models.BasicMapping.Default.ID;
                RM.Read(obj);
                Assert.AreEqual(obj,Models.BasicMapping.Default,"DB object does not same as default instance");
            }

            [Test(Description = "Test nested select queries in separate connections")]
            [Timeout(10000)]
            public void T05_NestedEnumerator()
            {
                EnumCount = 0;
                foreach (Models.BasicMapping l1 in RecordIterator.Enum<Models.BasicMapping>(RM))
                {
                    foreach (Models.BasicMapping l2 in RecordIterator.Enum<Models.BasicMapping>(RM, Where.EQ("ID", l1.ID)))
                    {
                        Models.BasicMapping o = new NUnit_tests.Models.BasicMapping();
                        o.ID = l1.ID;
                        RM.Read(o);
                        Assert.AreEqual(o, l2);
                    }
                    EnumCount++;
                }
            }

            [Test(Description = "Test nested select queries in separate connections")]
            [Timeout(10000)]
            public void T06_TransactionalNestedEnumerator()
            {
                using (ManagerTransaction t =RM.BeginTransaction()) {
                    T05_NestedEnumerator();
                }
            }


            [Test(Description = "Select an object with where expression from DB")]
            public void T07_WhereCondition()
            {
                ArrayList a=new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM,Where.EQ("ID",Models.BasicMapping.Default.ID)).Fill(a);
                Assert.Contains(Models.BasicMapping.Default,a);
            }

            [Test(Description = "Select an object with where LIKE expression from DB")]
            public void T08_WhereLikeCondition()
            {
                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text","LIKE","%"+Models.BasicMapping.Default.ID.ToString()+"%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a);
            }

            [Test(Description = "Test is LIKE is case insensitive")]
            public void T09_WhereLikeCaseInsensitive()
            {
                if (prov is PgSqlSpecifics)
                    Assert.Inconclusive("PGSQL LIKE statement is case sensitive");

                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text", "LIKE", "%" + Models.BasicMapping.Default.ID.ToString().ToUpper() + "%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a,"The LIKE operator is case sensitive");
            }


            [Test(Description = "Select an object with where ILIKE expression from DB")]
            public void T10_WhereILikeCondition()
            {
                if (prov is SQLiteSpecifics)
                    Assert.Inconclusive("SQLITE does not have ILIKE statement");
                if (prov is MSSqlSpecifics)
                    Assert.Inconclusive("MS SQL does not have ILIKE statement");

                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text", "ILIKE", "%"+Models.BasicMapping.Default.ID.ToString().ToUpper()+"%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a,"Backend does not have ILIKE expression");
            }

            public void SingleSerialization()
            {
                Models.BasicMapping sobj = Models.BasicMapping.RandomValue;
                string XML = ReplicaManager.SerializeObject(RM, sobj, ReplicaManager.ReplicaLog.Operation.Write);
                Models.BasicMapping dobj = ReplicaManager.DeserializeObjectAs<Models.BasicMapping>(RM, XML);
                Assert.AreEqual(sobj, dobj, "Serialization failed");
            }

            [Test(Description = "Measure serialization speed")]
            public void T11_Serialization()
            {
                int i = 0 ;
                DateTime start = DateTime.Now;
                while (i++ < 1000)
                {
                    SingleSerialization();
                }
                TimeSpan duration = DateTime.Now - start;
                decimal rate = (i+1m) / (decimal)duration.TotalSeconds;
                //Assert.Fail("Serialization rate is {0} object(s) per second.", rate);
            }

            [Test(Description = "Autoincrement facility")]
            public void T12_AutoIncrement()
            {
                Models.AutoIncrementObj o1=new NUnit_tests.Models.AutoIncrementObj();
                Models.AutoIncrementObj o2=new NUnit_tests.Models.AutoIncrementObj();
                RM.Write(o1);
                RM.Write(o2);
                Assert.AreEqual(o1.AutoInc64, o2.AutoInc64);
                RM.Read(o1);
                RM.Read(o2);
                Assert.Less(o1.AutoInc64, o2.AutoInc64);
            }

            [Test(Description = "Select query with order by text field")]
            public void T13_OrderByText()
            {
                foreach ( Models.BasicMapping bm in RecordIterator.Enum<Models.BasicMapping>(RM,Where.OrderBy("Text")))
                {
                    break;
                }
            }

            [Test(Description = "Select query with where text field")]
            public void T14_WhereTextField()
            {
                Models.BasicMapping ft = null;
                foreach (Models.BasicMapping bm in RecordIterator.Enum<Models.BasicMapping>(RM, Where.EQ("Text", Models.BasicMapping.Default.Text)))
                {
                    ft = bm;
                    break;
                }
                Assert.NotNull(ft);
            }

            [Test(Description = "Limited result set statement")]
            public void T15_SelectLimit()
            {
                int cnt = 0;
                foreach (Models.BasicMapping bm in RecordIterator.Enum<Models.BasicMapping>(RM, Where.Limit(1)))
                {
                    cnt++;

                }
                Assert.AreEqual(cnt,1);
            }
            [Test(Description = "Handle decimal numbers")]
            public void T16_NumberHandling()
            {
                Models.BasicMapping o = Models.BasicMapping.RandomValue;
                o.NumberMoney = 5m;
                o.NumberDouble = 5d;
                string sa1 = o.NumberMoney.ToString();
                string sa2 = o.NumberDouble.ToString();
                RM.Write(o);
                Guid ID = o.ID;
                o = new Models.BasicMapping();
                o.ID = ID;
                RM.Read(o);
                string sb1 = o.NumberMoney.ToString();
                string sb2 = o.NumberDouble.ToString();
                Assert.AreEqual(sa1, sb1);
                Assert.AreEqual(sa2, sb2);
                o.NumberMoney = 5.1234m;
                o.NumberDouble = 5.1234d;
                sa1 = o.NumberMoney.ToString();
                sa2 = o.NumberDouble.ToString();

                RM.Write(o);
                o = new Models.BasicMapping();
                o.ID = ID;
                RM.Read(o);

                sb1 = o.NumberMoney.ToString();
                sb2 = o.NumberDouble.ToString();
                Assert.AreEqual(sa1, sb1);
                Assert.AreEqual(sa2, sb2);
            }

            [Test(Description = "Handle DateTime values")]
            public void T17_DateHandling()
            {
                DateTime t = DateTime.Today;
                Models.BasicMapping o = Models.BasicMapping.RandomValue;
                Guid ID = o.ID;
                o.TimeStamp = t;
                RM.Write(o);

                bool found=false;
                foreach (Models.BasicMapping oe in RecordIterator.Enum<Models.BasicMapping>(RM,Where.LE("TimeStamp",t)))
                {
                    if (oe.ID==o.ID)
                        found=true;
                }

                Assert.IsTrue(found);

                found = false;
                foreach (Models.BasicMapping oe in RecordIterator.Enum<Models.BasicMapping>(RM, Where.LE("TimeStamp", t.AddSeconds(-1))))
                {
                    if (oe.ID == o.ID)
                        found = true;
                }

                Assert.IsFalse(found);


            }

            [Test(Description = "Handle replication with SQLITE")]
            public void T20_ReplicationHandling()
            {
                using (ReplicaManager replicamgr1 = new ReplicaManager())
                {
                    replicamgr1.OnApplyReplica += new ReplicaManager.ApplyReplicaEvent(delegate(ReplicaManager.ReplicaLog log, RecordManager _manager)
                    {
                        log.SeqNO = 0;
                        _manager.Write(log);
                    });
                    replicamgr1.RegisterWithRecordManager(RM);

                    SQLiteSpecifics prov = new SQLiteSpecifics();
                    string replicadb = @"C:\temp\replica.db3";
                    File.Delete(replicadb);
                    prov.ExtendConnectionString("Data Source", replicadb);
                    prov.ExtendConnectionString("BinaryGUID", "FALSE");
                    List<Type> types = new List<Type>();
                    foreach (InTable t in RM.RegisteredTypes)   
                        if (t.WithReplica) types.Add(t.BaseType);

                    using (RecordManager RM2 = new RecordManager(prov, types.ToArray()))
                    {

                        using (ReplicaManager replicamgr2 = new ReplicaManager())
                        {
                            replicamgr2.RegisterWithRecordManager(RM2);
                        

                        List<Models.BasicMapping> replobjs = new List<Models.BasicMapping>();

                        using (ManagerTransaction t = RM2.BeginTransaction())
                        {
                            while (replobjs.Count < 100)
                            {
                                Models.BasicMapping o = Models.BasicMapping.RandomValue;
                                replobjs.Add(o);
                                RM2.Write(o);
                            }
                            t.Commit();
                        }

                        using (ManagerTransaction t = RM.BeginTransaction())
                        {

                            foreach (Models.BasicMapping o in replobjs)
                            {
                                Models.BasicMapping n = new Models.BasicMapping();
                                n.ID = o.ID;
                                Assert.IsFalse(RM.Read(n));
                            }
                            t.Commit();
                        }

                            long rid = 0;
                            while (true) {
                            byte[] rbuf=replicamgr2.BuildReplicaBuffer(RM2,ref rid);
                                if (rbuf==null) break;
                            replicamgr1.ApplyReplicaBuffer(RM,rbuf);
                            }

                            using (ManagerTransaction t = RM.BeginTransaction())
                            {

                                foreach (Models.BasicMapping o in replobjs)
                                {
                                    Models.BasicMapping n = new Models.BasicMapping();
                                    n.ID = o.ID;
                                    Assert.IsTrue(RM.Read(n));
                                }
                                t.Commit();
                            }

                        }

                        
                        RM2.FlushConnectionPool();
                        RM2.Connection.Close();
                    }
                }

            }

            [Test(Description = "Handle timezones")]
            public void T21_DateTimeTZ()
            {
                DateTime t = DateTime.Today.ToLocalTime();
                Models.BasicMapping o = Models.BasicMapping.RandomValue;
                Guid ID = o.ID;
                o.TimeStamp = t;
                RM.Write(o);

                o = new NUnit_tests.Models.BasicMapping();
                o.ID = ID;
                RM.Read(o);

                Assert.AreEqual(o.TimeStamp.ToUniversalTime(), t.ToUniversalTime());
                t = DateTime.Today.ToUniversalTime();
                RM.Write(o);
                o = new NUnit_tests.Models.BasicMapping();
                o.ID = ID;
                RM.Read(o);

                Assert.AreEqual(o.TimeStamp.ToUniversalTime(), t.ToUniversalTime());

            }


            [TestFixtureTearDown]
            public void Cleanup()
            {
                RM.FlushConnectionPool();
                RM.Connection.Close();
            }

        }
    }
}
