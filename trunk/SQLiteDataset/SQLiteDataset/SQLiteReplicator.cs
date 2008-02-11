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
        private DbConnection replica=null;


        public DbConnection MasterDB
        {
            get {return master;}
            set {master=value; Open();}
    
        }

        public DbConnection ReplicaLog
        {
            get { return replica; }
            set { replica = value; Open(); }
        }

        public bool Ready
        {
            get
            {
                try
                {
                    return
                        (master.State == System.Data.ConnectionState.Open)
                        &&
                        (replica.State == System.Data.ConnectionState.Open);
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
            if (replica != null) replica.Close();
        }


public void Open()
{
    if ((master==null) || (replica==null)) return;

        try { master.Open(); }
        catch {};

        try { replica.Open(); }
        catch {};

    

    AttachReplicaDB();
    InitSchema();
}

private void AttachReplicaDB()
{
    using (DbCommand cmd = replica.CreateCommand())
        //try
        {
            cmd.CommandText = "ATTACH DATABASE \"@DBN\" as replica;";
            cmd.Parameters.Add(new SQLiteParameter("@DBN", ConnectionFileName(master)));
            cmd.ExecuteNonQuery();
        }
//        catch { }
       
}

        private string ConnectionFileName(DbConnection db)
        {
            DbConnectionStringBuilder dbb = new DbConnectionStringBuilder();
            dbb.ConnectionString = db.ConnectionString;
                return (string)dbb["Data Source"];
        }

        private void InitSchema()
        {

            using (DbCommand cmd = replica.CreateCommand())
            {
                
                cmd.CommandText = @"
create table IF NOT EXISTS peers (
peerid GUID unique primary key,
replicaid int64,
lastsync DateTime 
);
";
                cmd.ExecuteNonQuery();
cmd.CommandText = @"
insert into peers(rowid,peerid) values (1,2);
";
cmd.ExecuteNonQuery();

            }

        }

    }
}
