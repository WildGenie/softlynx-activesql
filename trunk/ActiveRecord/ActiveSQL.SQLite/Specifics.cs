using System;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Softlynx.ActiveSQL;
using System.Data.SQLite;

namespace Softlynx.ActiveSQL.SQLite
{
    public class SQLiteSpecifics : ProviderSpecifics
    {
        Hashtable sb = new Hashtable();
        
        public override DbConnection CreateDbConnection()
        {
            return new SQLiteConnection();
        }

        protected override  Hashtable CreateTypeMapping()
        {
            Hashtable res = new Hashtable();
            res[typeof(char)] = new object[] { "char", DbType.String };
            res[typeof(byte)] = new object[] { "byte", DbType.Byte };
            res[typeof(sbyte)] = new object[] { "sbyte", DbType.SByte };
            res[typeof(string)] = new object[] { "TEXT", DbType.String };
            res[typeof(Int16)] = new object[] { "smallint", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "int", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "bigint", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "TIMESTAMPTZ", DbType.DateTime };
            res[typeof(decimal)] = new object[] { "decimal", DbType.Decimal };
            res[typeof(float)] = new object[] { "float", DbType.Single };
            res[typeof(double)] = new object[] { "double", DbType.Double };
            res[typeof(bool)] = new object[] { "boolean", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "GUID", DbType.Guid };
            res[typeof(Object)] = new object[] { "BLOB", DbType.Binary };
            res[typeof(byte[])] = new object[] { "BLOB", DbType.Binary };
            return res;
        }


        public override DbParameter CreateParameter(string name, Type type)
        {
            DbParameter p = new SQLiteParameter();
            p.DbType = GetDbType(type);
            p.ParameterName = name;
            return p;
        }




        public override string AsFieldName(string s)
        {
            return string.Format("\"{0}\"", s);
        }

        public override string AsFieldParam(string s)
        {
            return string.Format("@{0}", s);
        }

        public override string AutoincrementStatement(string ColumnName)
        {
            return string.Format("{0} INTEGER PRIMARY KEY AUTOINCREMENT", AsFieldName(ColumnName));
        }

        

        public override void ExtendConnectionString(string key, string value)
        {
            sb.Add(key, value);
            string s = string.Empty;
            foreach (DictionaryEntry de in sb)
            {
                s += string.Format("{0}={1};", de.Key, de.Value);
            }
            Connection.ConnectionString = s;
        }

    }
}
