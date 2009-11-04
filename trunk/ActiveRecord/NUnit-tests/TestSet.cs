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
using NUnit.Framework;
using System.Reflection;

namespace NUnit_tests
{
    namespace Models
    {
        [InTable]
        public class BasicMapping : IDObject
        {
            public class Prop {
                static public PropType Text=new PropType<string>("Text field");
                static public PropType TimeStamp = new PropType<DateTime>("DateTime field");
                static public PropType Symbol = new PropType<char>("char field");
                static public PropType NumberByte = new PropType<byte>("byte field");
                static public PropType NumberShort = new PropType<short>("short field");
                static public PropType Number16 = new PropType<Int16>("INT16 field");
                static public PropType Number32 = new PropType<Int32>("INT32 field");
                static public PropType Number64 = new PropType<Int64>("INT64 field");
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
            
            //[ExcludeFromTable]
            public Decimal NumberMoney
            {
                get { return GetValue<Decimal>(Prop.NumberMoney, Decimal.Zero); }
                set { SetValue<Decimal>(Prop.NumberMoney, value); }
            }
            
            //[ExcludeFromTable]
            public Single NumberSingle
            {
                get { return GetValue<Single>(Prop.NumberSingle, Single.NaN); }
                set { SetValue<Single>(Prop.NumberSingle, value); }
            }

            //[ExcludeFromTable]
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

            public static BasicMapping Default {
                get {
                    if (_default == null)
                    {
                        Random r=new Random();
                        _default = new BasicMapping();
                        _default.ID = Guid.NewGuid();
                        _default.Text = "Sample Text Value " + _default.ID.ToString();
                        _default.TimeStamp = DateTime.Now;
                        _default.Symbol = 'a';
                        _default.NumberByte = (byte)r.Next(byte.MinValue,byte.MaxValue);
                        _default.NumberShort = (short)r.Next(short.MinValue, short.MaxValue);

                        _default.Number16 = (Int16)r.Next(Int16.MinValue, Int16.MaxValue);
                        _default.Number32 = (Int32)r.Next(Int32.MinValue, Int32.MaxValue);
                        _default.Number64 = (Int64)r.Next(Int32.MinValue, Int32.MaxValue);


                        _default.NumberMoney =decimal.Round((r.Next(1, Int16.MaxValue) + 1m) / (r.Next(1, Int16.MaxValue) + 1m),14);
                        _default.NumberSingle = (float)decimal.Round( (decimal)r.NextDouble(),6);
                        _default.NumberDouble = (double)decimal.Round((decimal)r.NextDouble(), 14);
                        _default.State = FileAccess.Write;
                        _default.Checkbox = true;
                        byte[] buf=new byte[40];
                        r.NextBytes(buf);
                        _default.BLOB = buf;

                    }
                    return _default;
                }
            }
        }
    }
    namespace Backends
    {

        [TestFixture("SQLITE", @"Data Source = c:\temp\test.db3;BinaryGUID=FALSE;")]
        [TestFixture("PGSQL", "host=localhost;Database=test;User Id=test;Password=test")]
        public class Backend
        {
            protected RecordManager RM = null;
            protected ProviderSpecifics prov = null;

            public Backend(string ProviderName, string ConnectionString)
            {
                Models.BasicMapping._default = null;
                if (ProviderName == "SQLITE")
                    prov = new SQLiteSpecifics();

                if (ProviderName == "PGSQL")
                    prov = new PgSqlSpecifics();

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
            }

            [Test(Description="Connects to database backend")]
            public void T01_ConnectDB()
            {
                    RM = new RecordManager(prov, new Type[] { typeof(Models.BasicMapping) });
                    Assert.NotNull(RM);
                    Assert.IsTrue(RM.Connection.State == ConnectionState.Open);
            }


            [Test(Description = "Write an object to database")]
            public void T02_WriteObject()
            {
                Models.BasicMapping obj = new NUnit_tests.Models.BasicMapping();
                obj.CopyFrom(Models.BasicMapping.Default);
                RM.Write(obj);
            }
            
            [Test(Description = "Read object back from DB")]
            public void T03_ReadObject()
            {
                Models.BasicMapping obj = new NUnit_tests.Models.BasicMapping();
                obj.ID = Models.BasicMapping.Default.ID;
                RM.Read(obj);
                Assert.AreEqual(obj,Models.BasicMapping.Default,"DB object does not same as default instance");
            }

            [Test(Description = "Select an object with where expression from DB")]
            public void T04_WhereCondition()
            {
                ArrayList a=new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM,Where.EQ("ID",Models.BasicMapping.Default.ID)).Fill(a);
                Assert.Contains(Models.BasicMapping.Default,a);
            }

            [Test(Description = "Select an object with where LIKE expression from DB")]
            public void T05_WhereLikeCondition()
            {
                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text","LIKE","%"+Models.BasicMapping.Default.ID.ToString()+"%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a);
            }

            [Test(Description = "Test is LIKE is case insensitive")]
            public void T06_WhereLikeCaseInsensitive()
            {
                if (prov is PgSqlSpecifics)
                    Assert.Inconclusive("PGSQL LIKE statement is case sensitive");

                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text", "LIKE", "%" + Models.BasicMapping.Default.ID.ToString().ToUpper() + "%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a,"The LIKE operator is case sensitive");
            }


            [Test(Description = "Select an object with where ILIKE expression from DB")]
            public void T07_WhereILikeCondition()
            {
                if (prov is SQLiteSpecifics)
                    Assert.Inconclusive("SQLITE does not have ILIKE statement");
                ArrayList a = new ArrayList();
                RecordIterator.Enum<Models.BasicMapping>(RM, Where.OP("Text", "ILIKE", "%"+Models.BasicMapping.Default.ID.ToString().ToUpper()+"%")).Fill(a);
                Assert.Contains(Models.BasicMapping.Default, a,"Backend does not have ILIKE expression");
            }

            [Test(Description = "Test serialization method")]
            public void T08_Serialization()
            {
                string XML = ReplicaManager.SerializeObject(RM, Models.BasicMapping.Default, ReplicaManager.ReplicaLog.Operation.Write);
                Models.BasicMapping obj = ReplicaManager.DeserializeObjectAs<Models.BasicMapping>(RM, XML);
                Assert.AreEqual(obj, Models.BasicMapping.Default, "Serialization failed");
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
