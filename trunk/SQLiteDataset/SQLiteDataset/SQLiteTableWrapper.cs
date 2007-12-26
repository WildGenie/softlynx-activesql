using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Collections;
using System.ComponentModel;
using System.Data.SQLite;

namespace Softlynx.SQLiteDataset
{
   
    /// <summary>
    ///  ласс-обертка дл€ представлени€ в качестве таблицы базы SQLite экземпл€ра DataTable
    /// </summary>
    public class SQLiteTableWrapper : Component
    {
        internal DataTable table = null;
        internal Container columns = new Container();

        public void AttachTable(DataTable SourceTable)
        {
            if (table != null) throw new Exception("Table already attached.");
            table = SourceTable;
            
            foreach (DataColumn column in table.Columns)
            {
                SQLiteColumnWrapper wrapper=new SQLiteColumnWrapper();
                wrapper.AttachColumn(column);
                columns.Add(wrapper,column.ColumnName);
            }
        }

        internal String ColumnsList(ICollection fromcolums, string columnprefix, string separator)
        {
            int i=0;
            String[] clist = new string[fromcolums.Count];
            foreach (Object c in fromcolums)
            {
                string cname = String.Empty;
                if (c is DataColumn) cname = (c as DataColumn).ColumnName;
                if (cname == String.Empty) throw new Exception("Can't get column name from type "+ fromcolums.GetType().ToString());
                clist[i++] = string.Format("{0}{1}", columnprefix, cname);
            };
            return String.Join(separator, clist);
        }

        internal String ColumnsList(ICollection fromcolums, string columnprefix)
        {
            return ColumnsList(fromcolums, columnprefix, ",");
        }

        internal String ColumnsList(ICollection fromcolums)
        {
            return ColumnsList(fromcolums, string.Empty, ",");
        }


        internal String CreateTableStatement()
        {
            String s = String.Format("CREATE TABLE IF NOT EXISTS {0} (\n", table.TableName);
            foreach (SQLiteColumnWrapper wrapper in columns.Components)
            {
                s += String.Format("{0},\n", wrapper.CreateColumnStatement());
            }
            s += String.Format("isdeleted flag default 0");
            if (table.PrimaryKey.Length > 0)
            {
                s += String.Format(",\nPRIMARY KEY ({0}) ON CONFLICT REPLACE",
                    ColumnsList(table.PrimaryKey)
                    );
            }
            s += String.Format("\n);\n");
            s += String.Format("CREATE INDEX IF NOT EXISTS {0}_DELETED_IDX on {0}(isdeleted);\n", table.TableName);
            return s;
        }

        internal string FillCommand(string filter,string orderby)
        {
          String s = String.Format("SELECT {0} from {1}", ColumnsList(table.Columns), table.TableName);
                if (filter != string.Empty)
                {
                    s += String.Format(" WHERE ({0})", filter);
                };

                if (orderby != string.Empty)
                {
                    s += String.Format(" ORDER BY ({0})", orderby);
                };
                return s;
        }

        internal string UpdateCommand()
        {
            return String.Format("INSERT INTO {0}({1}) values ({2})",
                table.TableName,
                ColumnsList(table.Columns),
                ColumnsList(table.Columns,"@")
                );
        }

        internal string DeleteCommand()
        {
            string pkeycolumns = string.Empty;
            foreach (DataColumn dc in table.PrimaryKey)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}=@{0}", dc.ColumnName);
            };
            if (pkeycolumns == String.Empty) throw new Exception("Can't mark record as deleted until primary key defined");
            return String.Format("UPDATE {0} SET isdeleted=1 where ({1})",
                table.TableName,
                pkeycolumns
                );
        }

    }
}
