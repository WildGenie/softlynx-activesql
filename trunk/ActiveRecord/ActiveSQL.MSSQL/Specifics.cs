using System;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Softlynx.ActiveSQL;
using System.Data.SqlClient;
using System.Data.Sql;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;

namespace Softlynx.ActiveSQL.MSSQL
{
    public class MSSqlSpecifics : ProviderSpecifics
    {
        SqlConnectionStringBuilder sb = new SqlConnectionStringBuilder();

        public override DbConnection CreateDbConnection()
        {
            return new SqlConnection();
        }

        protected override Hashtable CreateTypeMapping()
        {
            Hashtable res = new Hashtable();
            res[typeof(char)] = new object[] { "char", DbType.String };
            res[typeof(byte)] = new object[] { "smallint", DbType.Byte };
            res[typeof(sbyte)] = new object[] { "smallint", DbType.SByte };
            res[typeof(string)] = new object[] { "nvarchar(1024)", DbType.String };
            res[typeof(Int16)] = new object[] { "smallint", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "integer", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "bigint", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "datetime", DbType.DateTimeOffset };
            res[typeof(decimal)] = new object[] { "decimal(38,15)", DbType.Decimal};
            res[typeof(float)] = new object[] { "real", DbType.Single };
            res[typeof(double)] = new object[] { "double precision", DbType.Double };
            res[typeof(bool)] = new object[] { "bit", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "uniqueidentifier", DbType.Guid };
            res[typeof(Object)] = new object[] { "varbinary(max)", DbType.Binary};
            res[typeof(byte[])] = new object[] { "varbinary(max)", DbType.Binary};
            return res;
        }

        public override DbParameter CreateParameter(string name, Type type)
        {
            SqlParameter p = new SqlParameter();
            p.ParameterName = name;
            p.DbType = GetDbType(type);
            p.Size = Int32.MaxValue;
            if (p.DbType == DbType.Decimal)
            {
                p.Scale = 15;
                p.Precision = 38;
            }
            return p;
        }

        public override DbParameter SetupParameter(DbParameter param, InField f)
        {
            SqlParameter p = (SqlParameter)param;
            p.Size = f.Size;
            if (p.Size == 0)
            {

                p.Size = Int32.MaxValue; 
                switch (GetDbType(f.FieldType))
                {
                    case DbType.Decimal:
                        if (f.Scale==0) 
                            f.Scale=15;
                        if (f.Precision == 0)
                            f.Precision = 38;
                        //p.Size = Marshal.SizeOf(f.FieldType);
                        break;
                        
                        /*
                    case DbType.DateTime:
                    case DbType.DateTimeOffset:
                    case DbType.Int16:
                    case DbType.Int32:
                    case DbType.Int64:
                    case DbType.Binary:
                        p.Size = Int32.MaxValue;
                        break;



                    case DbType.String:
                        p.Size = Int32.MaxValue;
                        break;
                         */
                    default:
                        // p.Size = Marshal.SizeOf(f.FieldType);
                        break;
                }
            }
            p.Scale = f.Scale;
            p.Precision = f.Precision;
            
            return p;
        }

        

        public override string AsFieldName(string s)
        {
            return string.Format("[{0}]", s);
        }

        public override string AsFieldParam(string s)
        {
            return string.Format("@{0}", s);
        }
        

        public override string DropTableIfExists(string TableName)
        {
            return String.Format("IF EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}') DROP TABLE {1};", TableName,AsFieldName(TableName));
        }

        public override string AutoincrementStatement(string ColumnName)
        {
            return string.Format("{0} AUTOINCREMENT", AsFieldName(ColumnName));
        }

        public override void ExtendConnectionString(string key, string value)
        {
            sb.Add(key, value);
            Connection.ConnectionString = sb.ConnectionString;
        }

    }
}
