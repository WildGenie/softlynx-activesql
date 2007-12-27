using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Collections;
using System.ComponentModel;

namespace Softlynx.SQLiteDataset
{
    internal delegate object InitField();
    /// <summary>
    /// Класс-обертка для представления колонки в базы SQLite
    /// </summary>
    internal class SQLiteColumnWrapper : Component
    {
        static private Hashtable DbTypeMap=new Hashtable();

        event InitField OnNewRowInitEvent = null;

        static SQLiteColumnWrapper()
        {
            DbTypeMap[typeof(Guid)] = DbType.Guid;
            DbTypeMap[typeof(DateTime)] = DbType.DateTime;
            DbTypeMap[typeof(String)] = DbType.String;
            DbTypeMap[typeof(int)] = DbType.Int32;
            DbTypeMap[typeof(Int16)] = DbType.Int16;
            DbTypeMap[typeof(Int32)] = DbType.Int32;
            DbTypeMap[typeof(Int64)] = DbType.Int64;
            DbTypeMap[typeof(bool)] = DbType.Boolean;
            DbTypeMap[typeof(Boolean)] = DbType.Boolean;
            DbTypeMap[typeof(byte)] = DbType.Byte;
            DbTypeMap[typeof(Byte)] = DbType.Byte;
            DbTypeMap[typeof(Double)] = DbType.Double;
            DbTypeMap[typeof(float)] = DbType.Double;
            DbTypeMap[typeof(float)] = DbType.Double;

        }


        static internal DbType GetColumnDataType(DataColumn column)
        {
            Object res = DbTypeMap[column.DataType];
            if (res == null) 
                throw new Exception(
                    String.Format(
                    "Can't map data type {0} for {1}.{2}",
                    column.DataType.Name,
                    column.Table.TableName,
                    column.ColumnName
                    ));
            return (DbType)res;

        }

        internal static String CreateColumnStatement(DataColumn column)
        {
            string flags = string.Empty;
            if (!column.AllowDBNull) flags += " NOT NULL";
            if (column.Unique) flags += " UNIQUE ON CONFLICT REPLACE";
            if (column.DefaultValue!=DBNull.Value) flags += string.Format(" DEFAULT {0}",column.DefaultValue);
            return String.Format("{0} {1}{2}", column.ColumnName, column.DataType.Name,flags);
        }

        internal void AttachColumn(DataColumn column)
        {
            if (column.AllowDBNull == false)
            {
                if (column.DataType == typeof(Guid)) 
                {
                    OnNewRowInitEvent+=new InitField(GetNewGuidColumnValue);

                }
            }
        }

        internal object NewRowValue()
        {
            if (OnNewRowInitEvent != null)
            {
                return OnNewRowInitEvent();
            };
            return DBNull.Value;
        }



        private object GetNewGuidColumnValue()
        {
            return Guid.NewGuid();
        }
    }
}
