using System;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Softlynx.ActiveSQL;
using Npgsql;

namespace Softlynx.ActiveSQL.Postgres
{
    public class PgSqlSpecifics : IProviderSpecifics
    {
        NpgsqlConnectionStringBuilder sb = new NpgsqlConnectionStringBuilder();
        DbConnection db = new NpgsqlConnection();

        private static Hashtable CreateTypeMapping()
        {
            Hashtable res = new Hashtable();
            res[typeof(string)] = new object[] { "Text", DbType.String };
            res[typeof(Int16)] = new object[] { "smallint", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "integer", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "bigint", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "Timestamptz", DbType.DateTime };
            res[typeof(decimal)] = new object[] { "numeric", DbType.Decimal };
            res[typeof(double)] = new object[] { "double precision", DbType.Double };
            res[typeof(bool)] = new object[] { "boolean", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "Uuid", DbType.Guid };
            res[typeof(Object)] = new object[] { "bytea", DbType.Binary};
            res[typeof(byte[])] = new object[] { "bytea", DbType.Binary};
            return res;
        }

        Hashtable type_mapping = CreateTypeMapping();

        public DbParameter CreateParameter(string name, object value)
        {
            DbParameter p = new NpgsqlParameter();
            p.DbType = GetDbType(value.GetType());
            p.ParameterName = name;
            p.Value = value;
            return p;
        }

        public DbParameter CreateParameter(string name, Type type)
        {
            DbParameter p = new NpgsqlParameter();
            p.DbType = GetDbType(type);
            p.ParameterName = name;
            return p;
        }

        public DbParameter SetupParameter(DbParameter param, InField f)
        {
            return param;
        }

        public string GetSqlType(Type t)
        {
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum) 
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return "bytea";
            return (string)o[0];
        }
      

        public DbType GetDbType(Type  t)
        {
            object[] o = (object[])type_mapping[t];
            if (t.IsEnum)
                o = (object[])type_mapping[typeof(int)];
            if (o == null) return DbType.Object;
            return (DbType)o[1];
        }


        public string AsFieldName(string s)
        {
            return string.Format("\"{0}\"", s);
        }

        public string AsFieldParam(string s)
        {
            return string.Format(":{0}", s);
        }
        
        public string AutoincrementStatement(string ColumnName)
        {
            return string.Format("{0} BIGSERIAL", AsFieldName(ColumnName));
        }

        public DbConnection Connection
        {
            get {
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
