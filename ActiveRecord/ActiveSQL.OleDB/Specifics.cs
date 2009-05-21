using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

namespace Softlynx.ActiveSQL.OleDB
{
    public class OleDBSpecifics : IProviderSpecifics
    {
        OleDbConnectionStringBuilder sb = new OleDbConnectionStringBuilder();
        DbConnection db = new OleDbConnection();

        private static Hashtable CreateTypeMapping()
        {
        // http://msdn.microsoft.com/en-us/library/bb208866.aspx
            Hashtable res = new Hashtable();
            res[typeof(string)] = new object[] { "TEXT", DbType.String };
            res[typeof(Int16)] = new object[] { "SMALLINT", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "INTEGER", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "MONEY", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "DATETIME", DbType.DateTime };
            res[typeof(decimal)] = new object[] { "DECIMAL", DbType.Decimal };
            res[typeof(double)] = new object[] { "FLOAT", DbType.Double };
            res[typeof(bool)] = new object[] { "BIT", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "UNIQUEIDENTIFIER", DbType.Guid };
            res[typeof(Object)] = new object[] { "BINARY", DbType.Binary };
            res[typeof(byte[])] = new object[] { "BINARY", DbType.Binary };
            return res;
        }

        Hashtable type_mapping = CreateTypeMapping();

        public DbParameter CreateParameter(string name, object value)
        {
            DbParameter p = new OleDbParameter();
            p.DbType = GetDbType(value.GetType());
            p.ParameterName = name;
            p.Value = value;
            p.Size = 1024;
            return p;
        }

        public DbParameter CreateParameter(string name, Type type)
        {
            DbParameter p = new OleDbParameter();
            p.DbType = GetDbType(type);
            p.ParameterName = name;
            p.Size = 1024;
            return p;
        }


        public string GetSqlType(Type t)
        {
            throw new NotImplementedException();
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum)
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return "BINARY";
            return (string)o[0];
        }


        public DbType GetDbType(Type t)
        {
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum)
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return DbType.Object;
            return (DbType)o[1];
        }


        public string AsFieldName(string s)
        {
            return string.Format("[{0}]", s);
        }

        public string AsFieldParam(string s)
        {
            return string.Format("@{0}", s);
        }

        public string AutoincrementStatement(string ColumnName)
        {
            throw new NotImplementedException();
            //return string.Format("{0} BIGSERIAL", AsFieldName(ColumnName));
        }

        public DbConnection Connection
        {
            get
            {
                return db;
            }
        }

        public void ExtendConnectionString(string key, string value)
        {
            sb.Add(key, value);
            db.ConnectionString = sb.ConnectionString;

        }

        public string AdoptSelectCommand(string select, InField[] fields)
        {
            return select;
        }
    }
}
