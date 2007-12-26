using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Sql;
using System.Data.Common;
using System.Collections;
using System.ComponentModel;
using System.Data.SQLite;

namespace Softlynx.SQLiteDataset
{
    public class SQLiteDatasetWrapper:Component
    {
        protected DataSet dataset = null;
        protected Container tables = new Container();
        protected DbConnection connection = null;
        
        /// <summary>
        /// Связывает экземпляр DataSet с базой SQLite.
        /// Структура базы данных создается автоматически по схеме DataSet.
        /// При этом все изменения в экземпляре SourceDataSet (вставка, правка, удаление) мгновенно отражаются в базе SQLite.
        /// </summary>
        /// <param name="SourceDataSet">Ссылка на экземпляр объекта DataSet</param>
        /// <param name="Connection">Строка одключения к базе SQLite</param>
        public void AttachDataset(DataSet SourceDataset,DbConnection Connection)
        {
            if (dataset != null) throw new Exception("Dataset already attaced.");
            dataset = SourceDataset;
            connection = Connection;
            
            string SqlInitScript = String.Empty;

            foreach (DataTable table in dataset.Tables)
            {
                table.RowChanged += new DataRowChangeEventHandler(table_RowChanged);
                table.RowDeleting += new DataRowChangeEventHandler(table_RowChanged);
                
                SQLiteTableWrapper wrapper = new SQLiteTableWrapper();
                wrapper.AttachTable(table);
                tables.Add(wrapper, table.TableName);
                SqlInitScript += string.Format("{0}\n", wrapper.CreateTableStatement());
            }
            using (DbCommand cmd = connection.CreateCommand())
            {
                using (DbTransaction transaction = connection.BeginTransaction())
                {
                    cmd.CommandText = SqlInitScript;
                    cmd.ExecuteNonQuery();
                    transaction.Commit();
                };
            };
        }

        void table_RowChanged(object sender, DataRowChangeEventArgs e)
        {
            if (e.Action == DataRowAction.Add)
            {
                InsertOrUpdateRow(e.Row);
                e.Row.AcceptChanges();
            }

            if (e.Action == DataRowAction.Delete)
            {
                MarkRowDeleted(e.Row);
                e.Row.AcceptChanges();
            }

        }

        private void CheckWrapper(SQLiteTableWrapper wrapper, DataTable Table)
        {
            if (wrapper == null) throw new Exception("Can't find wrapper for table " + Table.TableName);
        }

        public void Fill(DataTable Table, string filter, object[] filter_params, string orderby)
        {
            SQLiteTableWrapper wrapper = tables.Components[Table.TableName] as SQLiteTableWrapper;
            CheckWrapper(wrapper, Table);
            using (DbTransaction transaction = connection.BeginTransaction(IsolationLevel.ReadUncommitted))
            {

                using (DbCommand cmd = connection.CreateCommand())
                {
                    cmd.CommandText = wrapper.FillCommand(filter, orderby);
                    cmd.Prepare();
                    int i = 0;
                    foreach (DbParameter param in cmd.Parameters)
                    {
                        param.Value = filter_params[i++];
                    }
                    Table.BeginLoadData();
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {

                        try
                        {
                            Table.Load(reader, LoadOption.OverwriteChanges);
                        }
                        finally
                        {
                             reader.Close();
                             transaction.Commit();
                        };
                    }
                    Table.EndLoadData();
                }
                
            }
        }

        /// <summary>
        /// Заполняет таблицу Table данными из базы SQLite. 
        /// Запрос исключает строки с признаком удаление isdeleted=1. 
        /// Сортировка отсутсвует.
        /// </summary>
        /// <param name="Table">Таблицу которую следует заполнить.</param>
        public void Fill(DataTable Table)
        {
            Fill(Table, "isdeleted=0", null, string.Empty);
        }

        /// <summary>
        /// Заполняет таблицу Table данными из базы SQLite. 
        /// Запрос исключает строки с признаком удаление isdeleted=1. 
        /// </summary>
        /// <param name="Table">Таблицу которую следует заполнить.</param>
        /// <param name="Table">Порядок сортировки в запросе (неесколько полей указываются ерез запятую).</param>
        public void Fill(DataTable Table, string orderby)
        {
            Fill(Table, "isdeleted=0", null, orderby);
        }



        internal void InsertOrUpdateRow(DataRow row)
        {
            SQLiteTableWrapper wrapper = tables.Components[row.Table.TableName] as SQLiteTableWrapper;
            CheckWrapper(wrapper,row.Table);
            using (DbCommand cmd = connection.CreateCommand())
            {
                cmd.CommandText = wrapper.UpdateCommand();
                foreach (DataColumn col in wrapper.table.Columns) {
                    cmd.Parameters.Add(new SQLiteParameter(
                        String.Format("@{0}",col.ColumnName),
                        row[col.ColumnName, DataRowVersion.Current]));
                };
                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }
        }

        internal void MarkRowDeleted(DataRow row)
        {
            SQLiteTableWrapper wrapper = tables.Components[row.Table.TableName] as SQLiteTableWrapper;
            CheckWrapper(wrapper, row.Table);

            using (DbCommand cmd = connection.CreateCommand())
            {
                cmd.CommandText = wrapper.DeleteCommand();

                foreach (DataColumn col in wrapper.table.PrimaryKey)
                {
                    cmd.Parameters.Add(new SQLiteParameter(
                        String.Format("@{0}", col.ColumnName),
                        row[col.ColumnName, DataRowVersion.Original]));
                };
                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }
        }

    }
}
