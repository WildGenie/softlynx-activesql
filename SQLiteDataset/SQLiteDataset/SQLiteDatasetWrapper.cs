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

        private DbConnection lastconnecton;
        private DataSet lastdataset;

        /// <summary>
        /// Подлючение к базе данных. Допускается только SQLite.
        /// </summary>
        public DbConnection SQLiteConnection
        {
            get { return lastconnecton; }
            set
            {
                lastconnecton = value;
                CheckAttch();
            }
        }
        /// <summary>
        /// DataSet который будет отображен в базу данных
        /// </summary>
        public DataSet LinkedDataSet
        {
            get { return lastdataset; }
            set
            {
                lastdataset = value;
                CheckAttch();
            }
        }

        private bool autoPopulateData = false;

        /// <summary>
        /// Определяет, нужно ли автоматически заполнять из базы данных все таблцы dataset
        /// </summary>
        public bool AutoPopulateData
        {
            get { return autoPopulateData; }
            set { 
                autoPopulateData = value;
                CheckAttch();
        }
        }

        private bool active;
        
        /// <summary>
        /// Признак активного состояния компонента
        /// </summary>
        public bool Active
        {
            get { return active; }
            set { 

                active = value;
                CheckAttch();
            }
        }

        private void CheckAttch()
        {
            if (lastconnecton != null)
                if (active) lastconnecton.Open(); 
                else lastconnecton.Close();
            if (!active) ClearLastState();
            if (
                (lastconnecton != null) &&
                (lastdataset != null) &&
                active
                )
                AttachDataset(lastdataset, lastconnecton);
        }

	


        protected Container tables = new Container();
        
        /// <summary>
        /// Связывает экземпляр DataSet с базой SQLite.
        /// Структура базы данных создается автоматически по схеме DataSet.
        /// При этом все изменения в экземпляре SourceDataSet (вставка, правка, удаление) мгновенно отражаются в базе SQLite.
        /// </summary>
        /// <param name="SourceDataSet">Ссылка на экземпляр объекта DataSet</param>
        /// <param name="Connection">Строка одключения к базе SQLite</param>
        public void AttachDataset(DataSet dataset,DbConnection connection)
        {
            if (!(connection is SQLiteConnection)) {
                throw new Exception("Only SQLite connection supported.");
            };
            lastdataset = dataset;
            lastconnecton = connection;

            ClearLastState();

       
            using (DbCommand cmd = connection.CreateCommand())
            {
                using (DbTransaction transaction = connection.BeginTransaction())
                {
                    try
                    {
                        foreach (DataTable table in dataset.Tables)
                        {
                            table.TableNewRow +=new DataTableNewRowEventHandler(table_TableNewRow);
                            table.RowChanged += new DataRowChangeEventHandler(table_RowChanged);
                            table.RowDeleting += new DataRowChangeEventHandler(table_RowChanged);
                            SQLiteTableWrapper wrapper = new SQLiteTableWrapper();
                            wrapper.AttachTable(table, connection);
                            tables.Add(wrapper, table.TableName);
                            wrapper.ReflectTableCreation();
                        }
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                    
                };
            };
            if (AutoPopulateData) PopulateDataSet(dataset);
        }

        private void ClearLastState()
        {
            while (tables.Components.Count > 0)
            {
                tables.Remove(tables.Components[0]);
            }
        }


        /// <summary>
        /// Заполняет весь датасет знаениями из связанной базы данных
        /// </summary>
        public void PopulateDataSet(DataSet dataset)
        {
        foreach (DataTable table in dataset.Tables) {
            Fill(table);
        };
        }

        void table_TableNewRow(object sender, DataTableNewRowEventArgs e)
        {
            SQLiteTableWrapper wrapper = MapTableWrapper(e.Row.Table); 
            wrapper.SetNewRowValues(e.Row);
        }

        void table_RowChanged(object sender, DataRowChangeEventArgs e)
        {
            if (e.Action == DataRowAction.Add)
            {
                InsertOrUpdateRow(e.Row);
                e.Row.AcceptChanges();
            }

            if (e.Action == DataRowAction.Change)
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

        private SQLiteTableWrapper MapTableWrapper(DataTable Table)
        {
            SQLiteTableWrapper wrapper = tables.Components[Table.TableName] as SQLiteTableWrapper;
            if (wrapper == null) throw new Exception(
                String.Format(
                "Can't find wrapper for table {0}",
                Table.TableName
                ));
            return wrapper;
        }


        public void Fill(DataTable Table, string filter, FillCmdParameter[] filter_params, string orderby)
        {
            SQLiteTableWrapper wrapper = MapTableWrapper(Table);
            wrapper.FillCommand(Table, filter, filter_params, orderby);
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
            SQLiteTableWrapper wrapper = MapTableWrapper(row.Table); 
            wrapper.ReflectRowModification(row);
        }

        internal void MarkRowDeleted(DataRow row)
        {
            SQLiteTableWrapper wrapper = MapTableWrapper(row.Table); 
            wrapper.ReflectRowDeletion(row);
        }

    }
}
