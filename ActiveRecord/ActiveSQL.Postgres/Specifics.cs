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
    public class PgSqlSpecifics : ProviderSpecifics
    {
        NpgsqlConnectionStringBuilder sb = new NpgsqlConnectionStringBuilder();

        public override DbConnection CreateDbConnection()
        {
            return new NpgsqlConnection();
        }

        protected override Hashtable CreateTypeMapping()
        {
            Hashtable res = new Hashtable();
            res[typeof(char)] = new object[] { "char", DbType.String };
            res[typeof(byte)] = new object[] { "smallint", DbType.Byte };
            res[typeof(sbyte)] = new object[] { "smallint", DbType.SByte };
            res[typeof(string)] = new object[] { "Text", DbType.String };
            res[typeof(Int16)] = new object[] { "smallint", DbType.Int16 };
            res[typeof(Int32)] = new object[] { "integer", DbType.Int32 };
            res[typeof(Int64)] = new object[] { "bigint", DbType.Int64 };
            res[typeof(DateTime)] = new object[] { "Timestamp", DbType.DateTime};
            res[typeof(decimal)] = new object[] { "numeric", DbType.Decimal };
            res[typeof(float)] = new object[] { "numeric", DbType.Single };
            res[typeof(double)] = new object[] { "numeric", DbType.Double };
            res[typeof(bool)] = new object[] { "boolean", DbType.Boolean };
            res[typeof(Guid)] = new object[] { "Uuid", DbType.Guid };
            res[typeof(Object)] = new object[] { "bytea", DbType.Binary};
            res[typeof(byte[])] = new object[] { "bytea", DbType.Binary};
            return res;
        }

        public override DbParameter CreateParameter(string name, Type type)
        {
            DbParameter p = new NpgsqlParameter();
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
            return string.Format(":{0}", s);
        }
        
        public override string AutoincrementStatement(string ColumnName)
        {
            return string.Format("{0} BIGSERIAL", AsFieldName(ColumnName));
        }

        public override void ExtendConnectionString(string key, string value)
        {
            sb.Add(key, value);
            Connection.ConnectionString = sb.ConnectionString;
        }

        /// <summary>
        /// Generate SQL code to alter table
        /// </summary>
        /// <param name="table">Table object</param>
        /// <param name="columnAction">Alteration kind</param>
        /// <param name="field">Field object</param>
        /// <returns>SQL code</returns>
        public override string AlterTableColumnSQL(InTable table, ColumnAction columnAction, InField field)
        {

            string code = "ALTER TABLE " + AsFieldName(table.Name);
            switch (columnAction)
            {
                case ColumnAction.Remove:
                    code += " DROP COLUMN " + AsFieldName(field.Name);
                    break;

                case ColumnAction.Recreate:
                    code += " DROP COLUMN " + AsFieldName(field.Name);
                    code += ", ";
                    code += " ADD COLUMN " + AsFieldName(field.Name) + " " + GetSqlType(field);
                    break;

                case ColumnAction.Insert:
                    code += " ADD COLUMN " + AsFieldName(field.Name) + " " + GetSqlType(field);
                    break;

                case ColumnAction.ChangeType:
                    code += " ALTER COLUMN " + AsFieldName(field.Name) + " TYPE " + GetSqlType(field);
                    break;

                default: code = null;
                    break;
            }
            return code;
        }


    }
}
