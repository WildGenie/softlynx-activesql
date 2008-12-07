using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data.Common;
using System.ComponentModel;
using System.IO;
using System.Xml;
using System.Xml.Serialization;
using Softlynx.Logger;

namespace Softlynx.SQLiteDataset.Replication
{

   public delegate void DatabaseUpdateEvent(object sender, UpdateEventArgs e);
   
    public class SQLiteReplicator : Component
    {
       private Guid OverrideMasterDbGuid = Guid.Empty;
        public Hashtable LastIDs= new Hashtable();
        private Hashtable TableColumns = new Hashtable();
        private DbConnection master=null;
        private Guid cached_guid = Guid.Empty;
       private Guid  dbVersion = Guid.Empty;
        DbCommand CheckReplicaExists_cmd = null;
        DbCommand FixReplicaLog_cmd = null;
        public bool HasSnapshot = false;
        public FileLogger logger = null;
        public event DatabaseUpdateEvent OnDatabaseUpdate=null;


        /// <summary>
        /// Задает максималный размер количества реплик в ReplicaSet
        /// </summary>
        public int MaxPortionSize = 250;

        /// <summary>
        /// Если в хеше IgnoreTable сущесвует значение ключа string равное имени таблицы то реплика пропускается. 
        /// null - не использовать фильтр по таблице
        /// </summary>
        public Hashtable IgnoreTable = new Hashtable();

        /// <summary>
        /// Если в хеше IgnoreAuthor сущесвует значние ключа Guid равное имени таблицы то реплика пропускается.
        /// null - не использоват фильтр по автору        
        /// </summary>
        public Hashtable IgnoreAuthor = new Hashtable();

        
        public DbConnection MasterDB
        {
            get {return master;}
            set {master=value; Open();}
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
        }

       public string ConnectionStringValue(string RequiredParam)
       {
           string result = null;
           RequiredParam=RequiredParam.ToUpper();

           foreach (string param in master.ConnectionString.Split(';'))
           {
               string[] kvp = param.Split('=');
               if (kvp.Length < 1) continue;
               if (kvp[0].ToUpper() == RequiredParam)
               {
                   result = string.Empty;
                   if (kvp.Length > 1) {
                       result = kvp[1].Trim('"');

                   break;
                   }
               }

           }
           return result;
       }

       public string ChangeConnectionStringValue(string ConnectionString,string NewParam,string NewValue)
       {
           string result = string.Empty;
           NewParam = NewParam.ToUpper();
           string[] cparams = ConnectionString.Split(';');

           foreach (string param in cparams)
           {
               string tmps=param;
               string[] kvp = param.Split('=');
               if (kvp.Length < 1) continue;
               if (kvp[0].ToUpper() == NewParam)
               {
                   if (kvp.Length > 1)
                   {
                       kvp[1] = NewValue;
                       tmps = String.Join("=", kvp);
                   }
               }
               result += tmps+";";
           }
           return result;
       }


       internal Hashtable GetDbInternalObjects(DbConnection conn) 
       {
           Hashtable intobjects=new Hashtable();
           
           DbCommand cmd = conn.CreateCommand();

           cmd.CommandText = @"
SELECT type,name,sql FROM sqlite_master
ORDER BY name;";

using (DbDataReader reader = cmd.ExecuteReader())
{
    while (reader.Read())
    {
        string ObjectType = reader.GetString(0);
        if (! (
            (ObjectType=="trigger")
            ||
            (ObjectType == "table")
            ||
            (ObjectType == "index")
            )) {continue;};

        string ObjectName = reader.GetString(1);
        string ObjectSql = string.Empty;

        string cs = string.Empty;
        string ds = string.Empty;

        try
        {
            ObjectSql=reader.GetString(2);
        }
        catch { }
        
        if (
            ObjectName.StartsWith("cleanup_replica_when_delete_")
            ||
            ObjectName.StartsWith("create_replica_when_")
            ||
            ObjectName.StartsWith("replica_changes_on_")
            )
        {
            cs = ObjectSql;
            ds = String.Format("drop {0} {1};", ObjectType, ObjectName);
        }

        if (cs != string.Empty)
        {
            string on = "create_" + ObjectType;
            String s= (string)intobjects[on];
            if (s==null) s=string.Empty;
            s+=cs+";\n";
            intobjects[on] = s;
        }
        
        if (ds != string.Empty)
        {
            string on = "drop_" + ObjectType;
            String s = (string)intobjects[on];
            if (s == null) s = string.Empty;
            s += ds+"\n";
            intobjects[on] = s;
        }

    }
    reader.Close();
}
return intobjects;
       }

       internal void DropDbInteralObjects(DbConnection conn,Hashtable DbInternals)
       {
           using (DbCommand cmd = conn.CreateCommand())
           {
               foreach (string ObjName in new string[] { "drop_index", "drop_trigger", "drop_table" })
               {
                   if (DbInternals.ContainsKey(ObjName))
                   {
                       cmd.CommandText = (string)DbInternals[ObjName];
                       cmd.ExecuteNonQuery();
                   };
               }
           }
       }

       internal void CreateDbInteralObjects(DbConnection conn, Hashtable DbInternals)
       {
           using (DbCommand cmd = conn.CreateCommand())
           {
               foreach (string ObjName in new string[] { "create_table", "create_index", "create_trigger"})
               {
                   if (DbInternals.ContainsKey(ObjName))
                   {
                       cmd.CommandText = (string)DbInternals[ObjName];
                       cmd.ExecuteNonQuery();
                   };
               }
           }
       }

       public void CreateSnapshot(string DestFile,params string[] EmptyTables)
       {
           using (DbTransaction transaction = master.BeginTransaction())
               try
               {
                   using (DbCommand cmd = master.CreateCommand())
                   {
                       Guid fake_replica = Guid.NewGuid();
                       cmd.Transaction = transaction;
                       cmd.CommandText = @"
insert into replica_log(replica_guid) values(@replica_guid);
";
                       cmd.Parameters.Add(new SQLiteParameter("@replica_guid", fake_replica));
                       cmd.ExecuteNonQuery();
                   };

                   long latest_seqno = GetLastKnownSeqNoForDB(SelfGuid);
                   string sourcefn = ConnectionStringValue("Data Source");
                   File.Copy(sourcefn, DestFile, true);
                   string snapshot_connectionstring = ChangeConnectionStringValue(master.ConnectionString, "Data Source", DestFile);
                   using (SQLiteConnection snapshot = new SQLiteConnection(snapshot_connectionstring))
                   {
                       snapshot.Open();
                       using (DbTransaction snapshot_transaction = snapshot.BeginTransaction())
                           try
                           {
                               using (DbCommand cmd = snapshot.CreateCommand())
                               {
                                   cmd.Transaction = snapshot_transaction;
                                   cmd.CommandText = @"select seqno from replica_log order by seqno desc limit 1";
                                   try
                                   {
                                       latest_seqno = (long)cmd.ExecuteScalar();
                                   }
                                   catch { };

                                   Hashtable dbobjects = GetDbInternalObjects(snapshot);

                                   DropDbInteralObjects(snapshot, dbobjects);

                                   foreach (string EmptyTable in EmptyTables)
                                   {
                                       cmd.CommandText = string.Format("delete from {0};", EmptyTable);
                                       cmd.ExecuteNonQuery();
                                   }

                                   cmd.CommandText = @"
delete from replica_log;
delete from replica_peer;

insert into replica_peer(rowid,peerid,replicaid,lastsync) values (1,@emptyguid,@latest_seqno,CURRENT_TIMESTAMP);
insert into replica_peer(peerid,replicaid,lastsync) values (@masterguid,@latest_seqno,CURRENT_TIMESTAMP)
";
                                   cmd.Parameters.Clear();
                                   cmd.Parameters.Add(new SQLiteParameter("@emptyguid", Guid.Empty));
                                   cmd.Parameters.Add(new SQLiteParameter("@latest_seqno", latest_seqno));
                                   cmd.Parameters.Add(new SQLiteParameter("@masterguid",(OverrideMasterDbGuid==Guid.Empty)?SelfGuid:OverrideMasterDbGuid));
                                   cmd.ExecuteNonQuery();
                                   CreateDbInteralObjects(snapshot, dbobjects);

                               }
                               snapshot_transaction.Commit();
                               using (DbCommand cmd = snapshot.CreateCommand())
                               {
                                   cmd.CommandText = @"
VACUUM;
ANALYZE;
";
                                   cmd.ExecuteNonQuery();
                               }

                           }
                           catch
                           {
                               snapshot_transaction.Rollback();
                               throw;
                           }
                       snapshot.Close();
                   };

                   SetLastKnownSeqNoForDB(SelfGuid, latest_seqno);

                   using (DbCommand cmd = master.CreateCommand())
                   {
                       cmd.Transaction = transaction;
                       cmd.CommandText = @"delete from replica_log where seqno<=@seqno";
                       cmd.Parameters.Add(new SQLiteParameter("@seqno", latest_seqno));
                       cmd.ExecuteNonQuery();

                   };
                   transaction.Commit();
                   using (DbCommand cmd = master.CreateCommand())
                   {
                       cmd.CommandText = @"
VACUUM;
ANALYZE;
";
                       cmd.ExecuteNonQuery();
                   }
               }
               catch
               {
                   transaction.Rollback();
                   throw;
               }
       }

public void Open()
{
    if ((master==null) ) return;
        if (master.State==System.Data.ConnectionState.Closed) 
            master.Open();
    InitReplicationSchema();
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


       public Guid DBVersionGuid
       {
           get
           {
               if (dbVersion == Guid.Empty)
               {
                   using (DbCommand cmd = master.CreateCommand())
                   {
                       cmd.CommandText = "select id from version";
                       Object obj = cmd.ExecuteScalar();
                       if (obj == null) return Guid.Empty;
                       dbVersion = (Guid)obj;
                   }
               }
               return dbVersion;
           }
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
                    cmd.CommandText = @"
update replica_peer 
    set 
        peerid=@peerid,
        lastsync=CURRENT_TIMESTAMP 
    where rowid=1";
                    cmd.Parameters.Add(new SQLiteParameter("@peerid", value));
                    int updated=cmd.ExecuteNonQuery();
                    if (updated == 0)
                    {
                     cmd.CommandText = @"
insert into replica_peer(rowid,peerid,lastsync) values(1,@peerid,CURRENT_TIMESTAMP);
";
                     cmd.ExecuteNonQuery();
                    }
                    
                    cached_guid = value;
                }
            
            }
        }

       public Guid SelfGuid1
       {
           get
           {
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
                   cmd.CommandText = @"
update replica_peer 
    set 
        peerid=@peerid,
        lastsync=CURRENT_TIMESTAMP 
    where rowid=1";
                   cmd.Parameters.Add(new SQLiteParameter("@peerid", value));
                   int updated = cmd.ExecuteNonQuery();
                   if (updated == 0)
                   {
                       cmd.CommandText = @"
insert into replica_peer(rowid,peerid,lastsync) values(1,@peerid,CURRENT_TIMESTAMP);
";
                       cmd.ExecuteNonQuery();
                   }

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

       /// <summary>
       /// Последнй добавленный серийный номер реплики
       /// </summary>
       public long LastReplicaSeqNo
       {
           get { try { return (long)LastIDs["replica_log"]; } catch { return 0; } }
       }

       /// <summary>
       /// Возвращяет последний известный номер репликации для указанной БД
       /// </summary>
       /// <param name="DbGuid">Глобальный идентификатор БД</param>
       /// <returns>Последний известнвй SeqNo для БД</returns>
       public long GetLastKnownSeqNoForDB(Guid DbGuid)
       {
           using (DbCommand cmd = master.CreateCommand())
           {
               cmd.CommandText = "select replicaid from  replica_peer where peerid=@DbGuid";
               cmd.Parameters.Add(new SQLiteParameter("@DbGuid", DbGuid));
               try
               {
                   return (long)cmd.ExecuteScalar();
               }
               catch {
                   return 0;
               }
           }
       }

       /// <summary>
       /// Устанавливает последний известный номер репликации для указанной БД
       /// </summary>
       /// <param name="DbGuid">Глобальный идентификатор БД</param>
       /// <param name="SeqNo">Последний известнвй SeqNo для БД</param>
       public void SetLastKnownSeqNoForDB(Guid DbGuid,long SeqNo)
       {
           using (DbCommand cmd = master.CreateCommand())
           {
               cmd.CommandText = @"
update replica_peer 
    set 
        lastsync=@lastsync,
        replicaid=@replicaid
    where       
        peerid=@peerid;
";
               cmd.Parameters.Add(new SQLiteParameter("@peerid", DbGuid));
               cmd.Parameters.Add(new SQLiteParameter("@replicaid", SeqNo));
               cmd.Parameters.Add(new SQLiteParameter("@lastsync", DateTime.Now));
               int updated=cmd.ExecuteNonQuery();
               if (updated == 0)
               {
                   cmd.CommandText = @"
insert into replica_peer(peerid,replicaid,lastsync) 
            values(@peerid,@replicaid,@lastsync);
";
                   cmd.ExecuteNonQuery();
               };
           }
       }


       /// <summary>
       /// Удаление записи о реплике
       /// </summary>
       /// <param name="SeqNo">Серийный номер реплики</param>
       public void RemoveReplica(long SeqNo)
       {
           using (DbCommand cmd = master.CreateCommand())
           {
               cmd.CommandText = "delete from replica_log where seqno=@SeqNo";
               cmd.Parameters.Add(new SQLiteParameter("@SeqNo",SeqNo));
               cmd.ExecuteNonQuery();
           }
       }

       //public void Check

        /// <summary>
        /// Проверяет и при необходимости создает таблицы и триггеры,
        /// необходимые для осуществления функций репликации
        /// </summary>
        private void InitReplicationSchema()
        {
            MasterDB.StateChange += new System.Data.StateChangeEventHandler(MasterDB_StateChange);
            MasterDB_StateChange(null, null);

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

            CreateReplicaTables();
            CreateVersionTables();

        }

       void SQLiteReplicator_Update(object sender, UpdateEventArgs e)
       {
           LastIDs[e.Table] = e.RowId;
           if (OnDatabaseUpdate != null) OnDatabaseUpdate(sender,e);
       }

       private SQLiteUpdateEventHandler updater = null;

       public SQLiteReplicator()
       {
           updater = new SQLiteUpdateEventHandler(SQLiteReplicator_Update);
       }


       void MasterDB_StateChange(object sender, System.Data.StateChangeEventArgs e)
       {
           if (
               (e==null)
                    ||
               (e.CurrentState==System.Data.ConnectionState.Open)
               )
           {
               if (MasterDB.DataSource != null)
               {
                   ((SQLiteConnection)MasterDB).Update -= updater;
                   ((SQLiteConnection)MasterDB).Update += updater;
               }
           }

           if (
    (e != null)
         &&
    (e.CurrentState != System.Data.ConnectionState.Open)
    )
           {
               //((SQLiteConnection)MasterDB).Update -= updater;
           }

       }

       public void CreateVersionTables()
       {
           using (DbCommand cmd = master.CreateCommand())
           {

               cmd.CommandText = "select count(*) from version";
               try
               {
                   cmd.ExecuteNonQuery();
               }
               catch
               {
                   Guid DBVersion = Guid.NewGuid();
                   cmd.CommandText = @"
create table IF NOT EXISTS Version (
id GUID 
);

insert into version (id) values (@DBVersion)";

                   cmd.Parameters.Add(new SQLiteParameter("@DBVersion", DBVersion));
                   cmd.ExecuteNonQuery();
               }
           }

       }
       
       public void CreateReplicaTables()
       {
            using (DbCommand cmd = master.CreateCommand())
            {
                
                cmd.CommandText = @"
create table IF NOT EXISTS replica_peer (
peerid GUID unique primary key,
replicaid INTEGER,
lastsync DATETIME 
);


-- таблица хранит данные по проводимым изменениям, первоначально заполняется триггером автоматически
-- drop table if exists replica_log; -- debug 

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
";        
                cmd.ExecuteNonQuery();
                if ((SelfGuid == Guid.Empty) && (OverrideMasterDbGuid==Guid.Empty)) SelfGuid = Guid.NewGuid();
            }

       }

        /// <summary>
        /// Удаление схемы для ведения записей об изменениях в таблице TableName
        /// </summary>
        /// <param name="TableName">Имя таблицы</param>
       public void DropTableReplicaLogSchema(string TableName)
        {
            Array r=master.GetSchema("Tables").Select(string.Format("Table_Name = 'replica_changes_on_{0}'",TableName));
           if (r.Length>0) using (DbCommand cmd = master.CreateCommand())
            {
                cmd.CommandText = String.Format(@"
delete from replica_log where record_rowguid in (select id from replica_changes_on_{0});
drop table if exists replica_changes_on_{0};
", TableName);

                cmd.ExecuteNonQuery();
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

create trigger if not exists create_replica_when_insert_{0} AFTER INSERT on {0}
BEGIN
insert into replica_log(table_name,record_rowguid,action) values ('{0}',NEW.id,'I');
insert into replica_changes_on_{0} select last_insert_rowid() as seqnoref,* from {0} where rowid=NEW.rowid;
END;  

create trigger if not exists create_replica_when_update_{0} AFTER UPDATE on {0}
BEGIN
delete from replica_log where record_rowguid=OLD.id and action='U';
insert into replica_log(table_name,record_rowguid,action) values ('{0}',OLD.id,'U');
insert into replica_changes_on_{0} select last_insert_rowid() as seqnoref,* from {0} where rowid=OLD.rowid;
END;  

create trigger if not exists create_replica_when_delete_{0} AFTER DELETE on {0}
BEGIN
delete from replica_log where record_rowguid=OLD.id and action='U';
insert into replica_log(table_name,record_rowguid,action) values ('{0}',OLD.ID,'D');
END;  

create trigger if not exists cleanup_replica_when_delete_{0}_replica_log BEFORE DELETE on replica_log
BEGIN
delete from replica_changes_on_{0} where seqnoref=old.seqno;
END;

", TableName);

                cmd.ExecuteNonQuery();
            }
        }
       
       /// <summary>
       /// Представлят массив buf как utf-8 строку.
       /// Используется для отладочных целей.
       /// </summary>
       /// <param name="buf">Исходный массив byte[]</param>
       /// <returns>Текст в кодировке utf-8</returns>
       private string XmlStrFromBuffer(byte[] buf)
       {
           UTF8Encoding enc = new UTF8Encoding();
           return enc.GetString(buf, 0, buf.Length);
       }


       
       /// <summary>
       /// Создается буффер содержащий сериализованный объект  ReplicaPortion
       /// с данными по локальной реплике с номером изменений большим чем LastKnownSeqNo,
       /// при этом игнорируются реплики по таблицам указанным в IgnoreTable и реплики от 
       /// авторов, указанных в  IgnoreAuthor
       /// </summary>
       /// <param name="LastKnownSeqNo">Последний известнвй номер реплики. 0 если нужно все.</param>
       /// <returns>Сериализованный в XML объект ReplicaPortion</returns>
       public byte[] BuildReplicaBuffer(ref Int64 LastKnownSeqNo)
       {
           if (
              HasSnapshot &&
             (LastKnownSeqNo < GetLastKnownSeqNoForDB(SelfGuid))
             )
           {
               Exception ex = new Exception("A newer snapshot required");
               throw ex;
           }
           byte[] result = null;
           ReplicaPortion rp = new ReplicaPortion();
           long initial_seqno = LastKnownSeqNo;
           rp.RequestLog(this, ref LastKnownSeqNo);
           String s = string.Empty;
           if (rp.ReplicaSet.Length > 0)
           {
               XmlSerializer formatter = new XmlSerializer(typeof(ReplicaPortion));
               using (MemoryStream strm = new MemoryStream())
               {
                   formatter.Serialize(strm, rp);
                   result = strm.ToArray();
                   strm.Close();
               }
                s = XmlStrFromBuffer(result);
            

           }
           if (initial_seqno != LastKnownSeqNo)
           {
               LogMessage("New replica buffer for seqno from {0} to {1} :: {2}", 
                   initial_seqno, 
                   LastKnownSeqNo, 
                   s);
           };
           rp = null;
           return result;
       }

        

        /// <summary>
        /// Применяет к текущей БД данный по репликации в буффере ReplicaBuffer
        /// </summary>
        /// <param name="ReplicaBuffer">Сериализованный в XML объект ReplicaPortion</param>
        public int ApplyReplicaBuffer(byte[] ReplicaBuffer)
        {
            if (ReplicaBuffer == null) return 0;
            String s = XmlStrFromBuffer(ReplicaBuffer);
            LogMessage("Apply buffer {0}", s);
            int apc = 0;
            XmlSerializer formatter = new XmlSerializer(typeof(ReplicaPortion));
            using (MemoryStream strm = new MemoryStream(ReplicaBuffer))
            {
                ReplicaPortion rp = (ReplicaPortion)formatter.Deserialize(strm);
                apc=rp.ApplyLog(this);
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
                ArrayList colnames = new ArrayList();

                using (DbCommand cmd = MasterDB.CreateCommand())
                {
                    cmd.CommandText = String.Format("PRAGMA table_info({0});", TableName);
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            colnames.Add(reader.GetString(1));
                        };
                        reader.Close();
                    }
                }
                result=(String[])colnames.ToArray(typeof(String));
                TableColumns[TableName] = result;
            }
            return result;
        }

       public void SetMasterDbGuid(Guid DbGuid)
       {
           OverrideMasterDbGuid = DbGuid;
       }

        public void LogMessage(string format, params Object[] args)
        {
            LogMessage(string.Format(format, args));
        }


       public void LogMessage(string s)
       {
           if (logger!=null) logger.LogMessage(s);
       }
    }
}
