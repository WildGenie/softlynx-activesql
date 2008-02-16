using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data.Sql;
using System.Data.Common;
using System.Data.SqlClient;
using System.ComponentModel;
using System.Runtime.Serialization;
using System.IO;
using System.Xml;
using System.Xml.Serialization;


namespace Softlynx.SQLiteDataset.Replication
{

   public class SQLiteReplicator : Component
    {
        internal Hashtable LastIDs= new Hashtable();
        private Hashtable TableColumns = new Hashtable();
        private DbConnection master=null;
        private string replica_filename = string.Empty;
        private Guid cached_guid = Guid.Empty;
        DbCommand CheckReplicaExists_cmd = null;
        DbCommand FixReplicaLog_cmd = null;

        
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
    InitReplicationSchema();
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


        internal bool IsReplicaExists(Guid ReplicaGuid)
        {
            CheckReplicaExists_cmd.Parameters[0].Value = ReplicaGuid;
            Object r=CheckReplicaExists_cmd.ExecuteScalar();
            return  r!= null;
        }

        /// <summary>
        /// Корректирует запись о репликации
        /// </summary>
        /// <param name="rowid">Исходный rowid в таблице replica_log</param>
        /// <param name="Stamp">Дата и время создания реплики</param>
        /// <param name="Author">Оригинальный автор реплики</param>
        /// <param name="ReplicaGuid">Уникальный код реплики</param>
        internal void FixReplicaLog(long SeqNo, DateTime Stamp, Guid Author, Guid ReplicaGuid)
        {

            FixReplicaLog_cmd.Parameters[0].Value=Stamp;
            FixReplicaLog_cmd.Parameters[1].Value=Author;
            FixReplicaLog_cmd.Parameters[2].Value=ReplicaGuid;
            FixReplicaLog_cmd.Parameters[3].Value = SeqNo;
            FixReplicaLog_cmd.ExecuteNonQuery();
        }

        void SQLiteReplicator_Update(object sender, UpdateEventArgs e)
        {
            LastIDs[e.Table] = e.RowId;
        }


        /// <summary>
        /// Проверяет и при необходимости создает таблицы и триггеры,
        /// необходимые для осуществления функций репликации
        /// </summary>
        private void InitReplicationSchema()
        {

            (MasterDB as SQLiteConnection).Update += new SQLiteUpdateEventHandler(SQLiteReplicator_Update);

            CheckReplicaExists_cmd = master.CreateCommand();
            CheckReplicaExists_cmd.CommandText = @"
select 1 from replica_log where replica_guid=@replica_guid
";
            CheckReplicaExists_cmd.Parameters.Add(new SQLiteParameter("@replica_guid"));
            CheckReplicaExists_cmd.Prepare();

            FixReplicaLog_cmd = master.CreateCommand();
            FixReplicaLog_cmd.CommandText = @"
update replica_log
    set 
        stamp=@stamp,
        author=@author,
        replica_guid=@replica_guid
    where seqno=@seqno;
";
            FixReplicaLog_cmd.Parameters.Add(new SQLiteParameter("@stamp"));
            FixReplicaLog_cmd.Parameters.Add(new SQLiteParameter("@author"));
            FixReplicaLog_cmd.Parameters.Add(new SQLiteParameter("@replica_guid"));
            FixReplicaLog_cmd.Parameters.Add(new SQLiteParameter("@seqno"));
            FixReplicaLog_cmd.Prepare();


            using (DbCommand cmd = master.CreateCommand())
            {
                
                cmd.CommandText = @"
create table IF NOT EXISTS replica_peer (
peerid GUID unique primary key,
replicaid INTEGER,
lastsync DATETIME 
);


-- таблица хранит данные по проводимым изменениям, первоначально заполняется триггером автоматически
drop table if exists replica_log;

create table IF NOT EXISTS replica_log (
seqno INTEGER PRIMARY KEY AUTOINCREMENT,
table_name TEXT,
record_rowguid GUID,  -- глобальный ункальный идентификатор изменяемой  записи
action INTEGER,
author GUID, -- идентификатор автора изменений, если NULL то это мы сами
stamp INTEGER default current_timestamp,
replica_guid GUID  -- уникальный код реплики (для быстрой проверки на наличие реплики в БД)
);

-- индекс код реплики для быстрой проверки на наличие реплики в БД
create index IF NOT EXISTS replica_log_replica_guid on replica_log(replica_guid);
create index IF NOT EXISTS replica_log_replica_table_name on replica_log(table_name);
create index IF NOT EXISTS replica_log_replica_record_rowid on replica_log(record_rowguid);


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
create index if not exists replica_changes_on_{0}_id on replica_changes_on_{0}(id);

DROP TRIGGER IF EXISTS when_insert_{0}; -- comment it on producion
DROP TRIGGER IF EXISTS when_update_{0}; -- comment it on producion
DROP TRIGGER IF EXISTS when_delete_{0}; -- comment it on producion
DROP TRIGGER IF EXISTS when_delete_{0}_replica_log; -- comment it on producion

create trigger if not exists when_insert_{0} AFTER INSERT on {0}
BEGIN
insert into replica_log(table_name,record_rowguid,action) values ('{0}',NEW.id,'I');
insert into replica_changes_on_{0} select last_insert_rowid() as seqnoref,* from {0} where rowid=NEW.rowid;
END;  


create trigger if not exists when_update_{0} AFTER UPDATE on {0}
BEGIN
delete from replica_log where record_rowguid=OLD.id and action='U';
insert into replica_log(table_name,record_rowguid,action) values ('{0}',OLD.id,'U');
insert into replica_changes_on_{0} select last_insert_rowid() as seqnoref,* from {0} where rowid=OLD.rowid;
END;  

create trigger if not exists when_delete_{0} AFTER DELETE on {0}
BEGIN
delete from replica_log where record_rowguid=OLD.id and action='U';
insert into replica_log(table_name,record_rowguid,action) values ('{0}',OLD.ID,'D');
END;  

create trigger if not exists when_delete_{0}_replica_log BEFORE DELETE on replica_log
BEGIN
delete from replica_changes_on_{0} where seqnoref=old.seqno;
END;

", TableName);

                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Создается буффер содержащий сериализованный объект  ReplicaPortion
        /// с данными по локальной реплике с номером изменений большим чем LastKnownSeqNo
        /// </summary>
        /// <param name="LastKnownSeqNo">Последний известнвй номер реплики. 0 если нужно все.</param>
        /// <returns></returns>
        public byte[] BuildLocalReplicaBuffer(ref Int64 LastKnownSeqNo)
        {
            byte[] result = null;
                ReplicaPortion rp = new ReplicaPortion();
                rp.RequestLog(this, ref LastKnownSeqNo);
                if (rp.ReplicaSet.Count > 0)
                {
                    IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                    using (MemoryStream strm = new MemoryStream())
                    {
                        formatter.Serialize(strm, rp);
                        result = strm.ToArray();
                        strm.Close();
                    }
                }
                rp = null;
                return result;
        }

        /// <summary>
        /// Применяет к текущей БД данный по репликации в бефере ReplicaBuffer
        /// </summary>
        /// <param name="ReplicaBuffer">Сериализованный бинарно объект ReplicaPortion</param>
        public int ApplyReplicaBuffer(byte[] ReplicaBuffer)
        {
            if (ReplicaBuffer == null) return 0;
            int apc = 0;
            IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            //IFormatter formatter = new System.Xml.Serialization. XmlSerializer();
            using (MemoryStream strm = new MemoryStream(ReplicaBuffer))
            {
                ReplicaPortion rp = (ReplicaPortion)formatter.Deserialize(strm);
                apc+=rp.ApplyLog(this);
            }
            return apc;
        }

        /// <summary>
        /// Возвращает массив строк с именами колонок таблицы TableName.
        /// Запрос в базу кэшируется, что позволяет многократно использовать эту функцию
        /// без повторного обращения к БД за схемой.
        /// </summary>
        /// <param name="TableName">Имя исследуемой таблицы</param>
        /// <returns></returns>
        internal String[] GetTableColumnNames(string TableName)
        {
            String[] result = (String[])TableColumns[TableName];
            if (result == null)
            {
                result=new String[0];

                using (DbCommand cmd = MasterDB.CreateCommand())
                {
                    cmd.CommandText = String.Format("PRAGMA table_info({0});", TableName);
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Array.Resize<String>(ref result, result.Length + 1);
                            result[result.Length - 1] = reader.GetString(1);
                        };
                        reader.Close();
                    }
                }

                TableColumns[TableName] = result;
            }
            return result;
        }
        
    }
}
