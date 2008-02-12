using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SQLite;
using System.Data.Sql;
using System.Data.Common;
using System.Data.SqlClient;
using System.ComponentModel;

namespace Softlynx.SQLiteReplicator
{
    public class SQLiteReplicator : Component
    {
        private DbConnection master=null;
        private string replica_filename = string.Empty;


        
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
                using (DbCommand cmd = master.CreateCommand())
                {
                cmd.CommandText = "select peerid from replica_peer where rowid=1";
                Object obj=cmd.ExecuteScalar();
                if (obj == null) return Guid.Empty;
                return (Guid)obj;
                }
            }
            set
            {
                using (DbCommand cmd = master.CreateCommand())
                {
                    cmd.CommandText = "insert or replace into replica_peer(peerid,rowid,lastsync) values (@myguid,1,CURRENT_TIMESTAMP)";
                    cmd.Parameters.Add(new SQLiteParameter("@myguid", value));
                    cmd.ExecuteNonQuery();
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
action INTEGER,
stamp DateTime default CURRENT_TIMESTAMP
);

";        cmd.ExecuteNonQuery();

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
