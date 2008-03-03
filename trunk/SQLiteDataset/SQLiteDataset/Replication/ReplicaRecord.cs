using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SQLite;
using System.Data.Common;
using System.ComponentModel;
using System.IO;
using System.Xml;
using System.Xml.Serialization;

namespace Softlynx.SQLiteDataset.Replication
{
    /// <summary>
    /// Описание одной записи репликации
    /// </summary>
    [Serializable]
    [XmlInclude(typeof(System.DBNull))]
    public class ReplicaRecord
    {

        Int64 seqno = 0;
        string tablename = string.Empty;
        char action = 'N';
        Object[] fields = null;
        Guid author = Guid.Empty;
        Guid rowguid = Guid.Empty;
        Guid replicaguid = Guid.Empty;
        DateTime stamp = DateTime.MinValue;

        /// <summary>
        /// Массив полей отдельно взятой записи
        /// null если не Insert или Update
        /// </summary>
        [XmlArray]
        public Object[] Fields
        {
            get { return fields; }
            set { fields = value; }
        }

        /// <summary>
        /// Тип реплики Insert/Update/Delete
        /// </summary>
        [XmlAttribute]
        public char Action
        {
            get { return action; }
            set { action = value; }
        }

        /// <summary>
        /// Имя таблицы по к которой относится данная запись
        /// </summary>
        [XmlAttribute]
        public string TableName
        {
            get { return tablename; }
            set { tablename = value; }
        }

        /// <summary>
        /// Уникальный идентификатор строки в таблице
        /// </summary>
        [XmlAttribute]
        public Int64 SeqNo
        {
            get { return seqno; }
            set { seqno = value; }
        }
        /// <summary>
        /// Уникальный идентификатор автора реплики.
        /// Служит для выявления авторства и предотвращения цикличности распространения данных.
        /// </summary>
        [XmlAttribute]
        public Guid Author
        {
            get { return author; }
            set { author = value; }
        }

        /// <summary>
        /// Время создания записи о реплике
        /// </summary>
        [XmlAttribute]
        public DateTime Stamp
        {
            get { return stamp; }
            set { stamp = value; }
        }

        /// <summary>
        /// Уникальный идентификатор модифицируемой строки.
        /// Назначается при первой репликации.
        /// </summary>
        [XmlAttribute]
        public Guid RowGuid
        {
            get { return rowguid; }
            set { rowguid = value; }
        }

        /// <summary>
        /// Уникальный идентификатор реплика строки.
        /// Назначается при первой репликации.
        /// </summary>
        [XmlAttribute]
        public Guid ReplicaGuid
        {
            get { return replicaguid; }
            set { replicaguid = value; }
        }

        /// <summary>
        /// Пустой конструктор для сериализации
        /// </summary>
        public ReplicaRecord()
        {
        }

        /// <summary>
        /// По текущей строке в reader таблицы replica_log создает объект реплики
        /// содержащий в себе все необходимые данные для воспроизведения операции в другой БД
        /// </summary>
        /// <param name="replicator">Родитеьский объект отвещающий за репликацию/param>
        /// <param name="reader">Строка replica_log</param>
        internal ReplicaRecord(SQLiteReplicator replicator, DbDataReader reader)
        {

            /*
            0:          seqno INTEGER PRIMARY KEY AUTOINCREMENT,
            1:          table_name text,
            2:          record_rowguid GUID,  -- глобальный ункальный идентификатор изменяемой  записи
            3:          action INTEGER,
            4:          author GUID, -- идентификатор автора изменений, если NULL то это мы сами
            5:          stamp INTEGER default CURRENT_TIMESTAMP,
            6:          replica_guid GUID     -- уникальный код реплики (для быстрой проверки на наличие реплики в БД)
            */

            SeqNo = reader.GetInt64(0);
            TableName = reader.GetString(1);
            RowGuid=reader.GetGuid(2);
            Action = reader.GetString(3)[0];
            if (!reader.IsDBNull(4))
                Author = reader.GetGuid(4);
            try
            {

                Stamp = reader.GetDateTime(5);
            }
            catch
            {
                Stamp = DateTime.Parse(reader.GetString(5));
            }
            if (reader.IsDBNull(6))
            {
                ReplicaGuid = Guid.NewGuid();
                Author = replicator.SelfGuid;
                replicator.FixReplicaLog(SeqNo, Stamp, Author, ReplicaGuid);
            } else 
                ReplicaGuid = reader.GetGuid(6);
        }

        internal void LoadReplicaValues(SQLiteReplicator replicator)
        {
            if ((action == 'I') || (action == 'U'))
            {
                using (DbCommand cmd = replicator.MasterDB.CreateCommand())
                {
                    cmd.Parameters.Clear();
                    cmd.CommandText = String.Format("select * from {0} where seqnoref=@seqno", SQLiteReplicator.ReplicaTable(TableName));
                    cmd.Parameters.Add(new SQLiteParameter("@seqno", SeqNo));
                    using (DbDataReader rowreader = cmd.ExecuteReader())
                    {
                        if (rowreader.Read())
                        {
                            Object[] temp_fields = new Object[rowreader.VisibleFieldCount];
                            rowreader.GetValues(temp_fields);
                            fields = new Object[rowreader.VisibleFieldCount - 1];
                            Array.Copy(temp_fields, 1, fields, 0, fields.Length);
                        }
                        rowreader.Close();
                    };

                }
            }
        }

        internal bool Apply(SQLiteReplicator replicator)
        {
            replicator.LogMessage("Try to apply replica ReplicaGuid:{0} RowGuid:{0} ", ReplicaGuid, RowGuid);
            if (replicator.IsReplicaExists(ReplicaGuid))
            {
                replicator.LogMessage("Replica exists");
                return false;
            }

            replicator.LastIDs.Clear();

            String[] columns=replicator.GetTableColumnNames(TableName);
            if (columns.Length <= 0) return false;

            using (DbCommand cmd = replicator.MasterDB.CreateCommand())
            {
                if (Action == 'I')
                {
                    string names = String.Empty;
                    string values = String.Empty;
                    foreach (String s in columns)
                    {
                        names += String.Format("{0},", s);
                        values += String.Format("@{0},", s);
                        cmd.Parameters.Add(new SQLiteParameter("@" + s));
                        cmd.Parameters[cmd.Parameters.Count - 1].Value = fields[cmd.Parameters.Count - 1];
                    }
                    names=names.TrimEnd(',');
                    values=values.TrimEnd(',');

                    cmd.CommandText = String.Format(@"
replace into {0}({1}) values({2});
", TableName, names, values);
                    cmd.ExecuteNonQuery();
                }

                if (Action == 'U')
                {
                    string expr = String.Empty;
                    foreach (String s in columns)
                    {
                        expr += String.Format("{0}=@{0},", s);
                        cmd.Parameters.Add(new SQLiteParameter("@" + s));
                        cmd.Parameters[cmd.Parameters.Count - 1].Value = fields[cmd.Parameters.Count - 1];
                    }
                    expr = expr.TrimEnd(',');

                    cmd.CommandText = String.Format(@"
update {0} set {1} where id=@rowguid
", TableName, expr);
                    cmd.Parameters.Add(new SQLiteParameter("@rowguid", RowGuid));
                    cmd.ExecuteNonQuery();
                }

                if (Action == 'D')
                {
                                        cmd.CommandText = String.Format(@"
delete from {0} where id=@rowguid
", TableName);
                                        cmd.Parameters.Add(new SQLiteParameter("@rowguid", RowGuid));
                    cmd.ExecuteNonQuery();
                }
            }

            try
            {
                long localseqno = (long)replicator.LastIDs["replica_log"];
                replicator.LogMessage("Fix ReplicaGuid:{0} at remote seqno:{1} to local seqno:{2}", ReplicaGuid, SeqNo,localseqno);
                replicator.FixReplicaLog(localseqno, Stamp, Author, ReplicaGuid);
            }
            catch { };

             return true;
        }

    }
}
