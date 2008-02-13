using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data.Sql;
using System.Data.Common;
using System.Data.SqlClient;
using System.ComponentModel;

namespace Softlynx.SQLiteReplicator
{
    /// <summary>
    /// Описание одной записи репликации
    /// </summary>
    class ReplicaRecord
    {

        Int64 seqno = 0;
        string tablename=string.Empty;
        char action = 'N';
        Object[] fields = null;
        Guid author=Guid.Empty;
        Guid rowguid=Guid.Empty;
        DateTime stamp=DateTime.MinValue;
                
        /// <summary>
        /// Массив полей отдельно взятой записи
        /// null если не Insert или Update
        /// </summary>
        public Object[] Fields
        {
            get { return fields;}
            set { fields = value; }
        }

        /// <summary>
        /// Тип реплики Insert/Update/Delete
        /// </summary>
        public char Action
        {
            get { return action; }
            set { action = value; }
        }

        /// <summary>
        /// Имя таблицы по к которой относится данная запись
        /// </summary>
        public string TableName
        {
            get { return tablename; }
            set { tablename = value; }
        }

        /// <summary>
        /// Уникальный идентификатор строки в таблице
        /// </summary>
        public Int64 SeqNo
        {
            get { return seqno; }
            set { seqno = value; }
        }
        /// <summary>
        /// Уникальный идентификатор автора реплики.
        /// Служит для выявления авторства и предотвращения цикличности распространения данных.
        /// </summary>
        public Guid Author
        {
            get { return author; }
            set { author = value; }
        }

        /// <summary>
        /// Время создания записи о реплике
        /// </summary>
        public DateTime Stamp
        {
            get { return stamp; }
            set { stamp = value; }
        }

        /// <summary>
        /// Уникальный идентификатор модифицируемой строки.
        /// Назначается при первой репликации.
        /// </summary>
        public Guid RowGuid
        {
            get { return rowguid; }
            set { rowguid = value; }
        }

        public ReplicaRecord(SQLiteReplicator replicator, DbDataReader reader)
        {

/*
seqno INTEGER PRIMARY KEY AUTOINCREMENT,
tableid INTEGER,
recordid INTEGER,
recordguid GUID,
action INTEGER,
author GUID, -- идентификатор автора изменений, если NULL то это мы сами
stamp INTEGER default CURRENT_TIMESTAMP
*/
            SeqNo =reader.GetInt64(0);
            TableName = reader.GetString(1);
            Int64 LocalRowID = reader.GetInt64(2);
            if (!reader.IsDBNull(3)) 
                RowGuid = reader.GetGuid(3);
            Action = reader.GetString(4)[0];
            if (!reader.IsDBNull(5))             
            Author = reader.GetGuid(5);
        try
        {

            Stamp = reader.GetDateTime(6);
        }
        catch
        {
                Stamp = DateTime.Parse(reader.GetString(6));
        }

            using (DbCommand cmd = replicator.MasterDB.CreateCommand())
            {
                if (RowGuid == Guid.Empty)
                {
                    RowGuid = Guid.NewGuid();
                    Author = replicator.SelfGuid;
                    cmd.CommandText = @"
update replica_log set 
recordguid=@recordguid,
author=@author,
stamp=@stamp
where seqno=@seqno";
                    cmd.Parameters.Add(new SQLiteParameter("@recordguid", RowGuid));
                    cmd.Parameters.Add(new SQLiteParameter("@author", Author));
                    cmd.Parameters.Add(new SQLiteParameter("@seqno", SeqNo));
                    cmd.Parameters.Add(new SQLiteParameter("@stamp", Stamp));
                    cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }

            }
            
        }

    }
    /// <summary>
    /// Порция данных реплик
    /// </summary>
    public class ReplicaPortion {
        ArrayList replicaset = new ArrayList();
        
        ArrayList ReplicaSet
        {
            get { return replicaset; }
            set { replicaset=value; }
        }


        /// <summary>
        /// Задает максималный размер количества реплик в ReplicaSet
        /// </summary>
        public int MaxPortionSize = 100;

    /// <summary>
    /// Заполняет ReplicaSet значениями реплик начиная с номера
    /// следующего за LastKnownSeqNo.
    /// Максимальное количество записей регламентируется перемнной MaxPortionSize.
    /// 
    /// </summary>
    /// <param name="replicator"></param>
    /// <param name="LastKnownSeqNo"></param>
    public void RequestLog(SQLiteReplicator replicator, Int64 LastKnownSeqNo)
        {
            using (DbTransaction transaction = replicator.MasterDB.BeginTransaction())
            {
                using (DbCommand cmd = replicator.MasterDB.CreateCommand())
                {
                    cmd.CommandText = "select * from replica_log where seqno>@seqno limit @maxrecords";
                    cmd.Parameters.Add(new SQLiteParameter("@seqno", LastKnownSeqNo));
                    cmd.Parameters.Add(new SQLiteParameter("@maxrecords", MaxPortionSize));
                    replicaset.Clear();
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            replicaset.Add(new ReplicaRecord(replicator, reader));
                        };
                        reader.Close();
                    }
                }
                transaction.Commit();
            }
        }

}

    public class SQLiteReplicator : Component
    {
        private DbConnection master=null;
        private string replica_filename = string.Empty;
        private Guid cached_guid = Guid.Empty;


        
        public DbConnection MasterDB
        {
            get {return master;}
            set {master=value; Open();}
        }

         public string ReplicaLogFN
        {
            get { return replica_filename; }
//            set { replica = value; Open(); }
        }
        
        public bool Ready
        {
            get
            {
                try
                {
                    return (master.State == System.Data.ConnectionState.Open);
                }
                catch { };
                return false;
            }
            set
            {
                if (value) Open(); else CloseConnections();
            }
        }

        private void CloseConnections()
        {
            if (master != null) master.Close();
//            if (replica != null) replica.Close();
        }


public void Open()
{
    if ((master==null) ) return;
        try { master.Open(); }
        catch {};
        replica_filename = System.IO.Path.ChangeExtension(ConnectionFileName(master),".dr3");
    AttachReplicaDB();
    InitSchema();
}

private void AttachReplicaDB()
{
    //System.IO.File.Delete(ReplicaLogFN);

    using (DbCommand cmd = master.CreateCommand())
        try
        {
            cmd.CommandText = "ATTACH DATABASE @DBN as replica;";
            cmd.Parameters.Add(new SQLiteParameter("@DBN", ReplicaLogFN));
            //cmd.ExecuteNonQuery();
        }
        catch { }
       
}

        static public string ConnectionFileName(DbConnection db)
        {
            DbConnectionStringBuilder dbb = new DbConnectionStringBuilder();
            dbb.ConnectionString = db.ConnectionString;
                return (string)dbb["Data Source"];
        }

        /// <summary>
        /// Генерирует для таблицы name имя таблицы-реплики в базе реплик
        /// </summary>
        /// <param name="name">Имя таблицы</param>
        /// <returns></returns>
        static public string ReplicaTable(string name)
        {
            return String.Format("replica_changes_on_{0}", name);
        }


        public Guid SelfGuid
        {
            get {
                if (cached_guid == Guid.Empty)
                {
                    using (DbCommand cmd = master.CreateCommand())
                    {
                        cmd.CommandText = "select peerid from replica_peer where rowid=1";
                        Object obj = cmd.ExecuteScalar();
                        if (obj == null) return Guid.Empty;
                        cached_guid = (Guid)obj;
                        
                    }
                }
                return cached_guid;
            }
            set
            {
                using (DbCommand cmd = master.CreateCommand())
                {
                    cmd.CommandText = "insert or replace into replica_peer(peerid,rowid,lastsync) values (@myguid,1,CURRENT_TIMESTAMP)";
                    cmd.Parameters.Add(new SQLiteParameter("@myguid", value));
                    cmd.ExecuteNonQuery();
                    cached_guid = value;
                }
            
            }
        }
        private void InitSchema()
        {

            using (DbCommand cmd = master.CreateCommand())
            {
                
                cmd.CommandText = @"
create table IF NOT EXISTS replica_peer (
peerid GUID unique primary key,
replicaid INTEGER,
lastsync DATETIME 
);

create table IF NOT EXISTS replica_log (
seqno INTEGER PRIMARY KEY AUTOINCREMENT,
tableid INTEGER,
recordid INTEGER,
recordguid GUID,
action INTEGER,
author GUID, -- идентификатор автора изменений, если NULL то это мы сами
stamp INTEGER default CURRENT_TIMESTAMP
);

create index IF NOT EXISTS replica_log_recordguid on replica_log(recordguid);

-- временная таблица ключ-значение досупная только в рамках этого соединения
create temp table vars(
name text primary key,
value object
);

-- Триггер для записи ID инициатора изменений
create temp trigger before_replica_log_insert AFTER INSERT on replica_log
BEGIN
update replica_log
SET author=(select value from vars where name='CurrentAuthorID' limit 1) 
where seqno=new.seqno and author isnull;
END;
";        
                cmd.ExecuteNonQuery();

            if (SelfGuid == Guid.Empty) SelfGuid = Guid.NewGuid();
            }
        }

        /// <summary>
        /// Создание схемы для ведения записей об изменениях в таблице TableName
        /// </summary>
        /// <param name="TableName">Имя таблицы</param>
        public void CreateTableReplicaLogSchema(string TableName)
        {
            using (DbCommand cmd = master.CreateCommand())
            {
                cmd.CommandText = String.Format(@"
create table if not exists replica_changes_on_{0} as 
    select 0 as seqnoref,* from {0} limit 0;

create index if not exists replica_changes_on_{0}_seqnoref on replica_changes_on_{0}(seqnoref);

DROP TRIGGER IF EXISTS when_insert_{0};
create trigger if not exists when_insert_{0} AFTER INSERT on {0}
BEGIN
insert into replica_log(tableid,recordid,action) values ('{0}',NEW.ROWID,'I');
insert into replica_changes_on_{0} select last_insert_rowid() as seqnoref,* from {0} where rowid=NEW.rowid;
END;  
", TableName);

                cmd.ExecuteNonQuery();
            }
        }

    }
}
