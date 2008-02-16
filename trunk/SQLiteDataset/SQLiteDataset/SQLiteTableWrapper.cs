using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Collections;
using System.ComponentModel;
using System.Data.Common;
using System.Data.SQLite;

namespace Softlynx.SQLiteDataset
{

    public class FillCmdParameter
    {
        internal string ParameterName = String.Empty;
        internal Object ParameterValue = null;
        public FillCmdParameter(String Name, Object Value)
        {
            ParameterName = Name;
            ParameterValue = Value;

        }
    }
    /// <summary>
    /// Класс-обертка для представления в качестве таблицы базы SQLite экземпляра DataTable
    /// </summary>
    internal class SQLiteTableWrapper : Component
    {
        internal Container columns = new Container();
        private DbCommand InsertCmd = null;
        private DbCommand UpdateCmd = null;
        private DbCommand DeleteCmd = null;
        private DbCommand FillCmd = null;
        private DbCommand CreateCmd = null;
        private string FillCmdBase = String.Empty;

        private string InsertCommandText(DataTable table)
        {
            return String.Format("INSERT INTO {0}({1}) values ({2})",
                table.TableName,
                ColumnsList(table.Columns),
                ColumnsList(table.Columns, "@")
                );
        }


        private string UpdateCommandText(DataTable table)
        {
            string pkeycolumns = string.Empty;
            string keyvalpairs = string.Empty;
            foreach (DataColumn dc in table.Columns)
            {
                if (keyvalpairs != String.Empty) keyvalpairs += ",";
                keyvalpairs += String.Format("{0}=@{0}", dc.ColumnName);
            };

            foreach (DataColumn dc in table.PrimaryKey)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}=@{0}", dc.ColumnName);
            };
            if (pkeycolumns == String.Empty) throw new Exception("Can't update record until primary key defined");

            return String.Format("UPDATE {0} set {1} where ({2})",
                table.TableName,
                keyvalpairs,
                pkeycolumns
                );
        }

        private string DeleteCommandText(DataTable table)
        {
            string pkeycolumns = string.Empty;
            foreach (DataColumn dc in table.PrimaryKey)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}=@{0}", dc.ColumnName);
            };
            if (pkeycolumns == String.Empty) throw new Exception("Can't mark record as deleted until primary key defined");
//            return String.Format("UPDATE {0} SET isdeleted=1 where ({1})",
            return String.Format("DELETE FROM {0} where ({1})",
                table.TableName,
                pkeycolumns
                );
        }

        private String CreateTableStatement(DataTable table)
        {
            String s = String.Format("CREATE TABLE IF NOT EXISTS {0} (\n", table.TableName);
            String cols = String.Empty;
            foreach (DataColumn col in table.Columns)
            {
                if (cols!=String.Empty) cols+=",\n";
                cols += String.Format("{0}", SQLiteColumnWrapper.CreateColumnStatement(col));
            }
            s+=cols;
//            s += String.Format("isdeleted flag default 0");
            if (table.PrimaryKey.Length > 0)
            {
                s += String.Format(",\nPRIMARY KEY ({0}) ON CONFLICT REPLACE",
                    ColumnsList(table.PrimaryKey)
                    );
            }
            s += String.Format("\n);\n");
            //s += String.Format("CREATE INDEX IF NOT EXISTS {0}_DELETED_IDX on {0}(isdeleted);\n", table.TableName);
            return s;
        }

        public void AttachTable(DataTable table, DbConnection connection)
        {
            foreach (DataColumn column in table.Columns)
            {
                SQLiteColumnWrapper wrapper=new SQLiteColumnWrapper();
                wrapper.AttachColumn(column);
                columns.Add(wrapper,column.ColumnName);
            }

            InsertCmd = connection.CreateCommand();
            InsertCmd.CommandText = InsertCommandText(table);

            foreach (DataColumn col in table.Columns)
            {
                InsertCmd.Parameters.Add(
                    new SQLiteParameter(
                    String.Format("@{0}",col.ColumnName),
                    SQLiteColumnWrapper.GetColumnDataType(col),
                    col.ColumnName
                    ));
            };
            InsertCmd.Prepare();

            UpdateCmd = connection.CreateCommand();
            UpdateCmd.CommandText = UpdateCommandText(table);

            foreach (DataColumn col in table.Columns)
            {
                UpdateCmd.Parameters.Add(
                    new SQLiteParameter(
                    String.Format("@{0}", col.ColumnName),
                    SQLiteColumnWrapper.GetColumnDataType(col),
                    col.ColumnName
                    ));
            };
            UpdateCmd.Prepare();


            DeleteCmd = connection.CreateCommand();
            DeleteCmd.CommandText = DeleteCommandText(table);
            foreach (DataColumn col in table.PrimaryKey)
            {
                DeleteCmd.Parameters.Add(new SQLiteParameter(
                    String.Format("@{0}", col.ColumnName),
                    SQLiteColumnWrapper.GetColumnDataType(col),
                    col.ColumnName
                    ));
            };
            DeleteCmd.Prepare();

            FillCmd = connection.CreateCommand();
            FillCmdBase = String.Format("SELECT {0} from {1}", ColumnsList(table.Columns), table.TableName);

            CreateCmd = connection.CreateCommand();
            CreateCmd.CommandText = CreateTableStatement(table);
            CreateCmd.Prepare();
        }

        public void ReflectRowInsert(DataRow row)
        {
            foreach (DbParameter param in InsertCmd.Parameters)
            {
                param.Value = row[param.SourceColumn, DataRowVersion.Current];
            }
            InsertCmd.ExecuteNonQuery();
        }

        public void ReflectRowUpdate(DataRow row)
        {
            foreach (DbParameter param in UpdateCmd.Parameters)
            {
                param.Value = row[param.SourceColumn, DataRowVersion.Current];
            }
            UpdateCmd.ExecuteNonQuery();
        }

        public void ReflectRowDeletion(DataRow row)
        {
            foreach (DbParameter param in DeleteCmd.Parameters)
            {
                param.Value = row[param.SourceColumn, DataRowVersion.Original];
            }
            DeleteCmd.ExecuteNonQuery();
        }

        public void ReflectTableCreation()
        {
            CreateCmd.ExecuteNonQuery();
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


        public void FillCommand(DataTable table,string filter,FillCmdParameter[] filter_params,string orderby)
        {
            FillCmd.Parameters.Clear();
            FillCmd.CommandText = FillCmdBase;
                if (filter != string.Empty)
                {
                    FillCmd.CommandText += String.Format(" WHERE ({0})", filter);
                };

                if (orderby != string.Empty)
                {
                    FillCmd.CommandText += String.Format(" ORDER BY ({0})", orderby);
                };

                using (DbTransaction transaction = FillCmd.Connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    if (filter_params!=null)
                        foreach (FillCmdParameter param in filter_params)
                        {
                            FillCmd.Parameters.Add(new SQLiteParameter(
                                param.ParameterName,
                                param.ParameterValue
                                ));
                        }

                    table.BeginLoadData();

                    using (DbDataReader reader = FillCmd.ExecuteReader())
                    {
                        try
                        {
                            table.Load(reader, LoadOption.OverwriteChanges);
                        }
                        finally
                        {
                            reader.Close();
                            transaction.Commit();
                            table.EndLoadData();
                        };
                    }
                }

        }

        private SQLiteColumnWrapper MapColumnWrapper(DataColumn Column)
        {
            SQLiteColumnWrapper wrapper = columns.Components[Column.ColumnName] as SQLiteColumnWrapper;
            if (wrapper == null) throw new Exception(
                String.Format(
                "Can't find wrapper for table {0} column {1}",
                Column.Table.TableName,
                Column.ColumnName));
            return wrapper;
        }


        internal void SetNewRowValues(DataRow row)
        {
            foreach (DataColumn col in row.Table.Columns)
            {
                SQLiteColumnWrapper wrapper = MapColumnWrapper(col);
                if (
                    (row[col]==null) 
                    ||
                    (row[col] == DBNull.Value) 
                    )
                    row[col] = wrapper.NewRowValue();
            }
        }
    }
}
