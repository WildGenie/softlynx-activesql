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
            res[typeof(DateTime)] = new object[] { "Timestamp", DbType.DateTime };
            res[typeof(Guid)] = new object[] { "Uuid", DbType.Guid };
            res[typeof(Object)] = new object[] { "bytea", DbType.Guid };
            return res;
        }

        Hashtable type_mapping = CreateTypeMapping();

        public DbParameter CreateParameter(InField f)
        {
            DbParameter p = new NpgsqlParameter();
            p.DbType = GetDbType(f);
            p.ParameterName = f.Name;
            return p;
        }

        public string GetSqlType(InField f)
        {
            object[] o = (object[])type_mapping[f.FieldType];
            if (o == null) return "bytea";
            return (string)o[0];
        }

        public DbType GetDbType(InField f)
        {
            object[] o = (object[])type_mapping[f.FieldType];
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
    }
}
