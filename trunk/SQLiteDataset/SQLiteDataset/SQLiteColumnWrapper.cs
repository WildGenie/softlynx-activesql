using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Collections;
using System.ComponentModel;

namespace Softlynx.SQLiteDataset
{
    /// <summary>
    ///  ласс-обертка дл€ представлени€ колонки в базы SQLite
    /// </summary>
    public class SQLiteColumnWrapper : Component
    {
        internal DataColumn column = null;

        internal String CreateColumnStatement()
        {
            string flags = string.Empty;
            if (!column.AllowDBNull) flags += " NOT NULL";
            if (column.Unique) flags += " UNIQUE ON CONFLICT REPLACE";
            if (column.DefaultValue!=DBNull.Value) flags += string.Format(" DEFAULT {0}",column.DefaultValue);
            return String.Format("{0} {1}{2}", column.ColumnName, column.DataType.Name,flags);
        }

        public void AttachColumn(DataColumn SourceClumn)
        {
            if (column != null) throw new Exception("Column already attached.");
            column = SourceClumn;
        }

    }
}
