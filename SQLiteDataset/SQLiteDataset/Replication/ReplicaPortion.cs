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

namespace Softlynx.SQLiteDataset.Replication
{
    /// <summary>
    /// Порция данных о репликах
    /// </summary>
    [Serializable]
    public class ReplicaPortion
    {
        ArrayList replicaset = new ArrayList();

        public ArrayList ReplicaSet
        {
            get { return replicaset; }
            set { replicaset = value; }
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
        public void RequestLog(SQLiteReplicator replicator, ref Int64 LastKnownSeqNo)
        {
            using (DbTransaction transaction = replicator.MasterDB.BeginTransaction(System.Data.IsolationLevel.Snapshot))
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
                            ReplicaRecord rr = new ReplicaRecord(replicator, reader);
                            replicaset.Add(rr);
                            LastKnownSeqNo = rr.SeqNo;
                        };
                        reader.Close();
                    }
                }
                transaction.Commit();
            }
        }

        public int ApplyLog(SQLiteReplicator replicator)
        {
            if (ReplicaSet.Count <= 0) return 0;
            int apc = 0;
            using (DbTransaction transaction = replicator.MasterDB.BeginTransaction(System.Data.IsolationLevel.Snapshot))
            {
                foreach (ReplicaRecord rr in ReplicaSet)
                {
                    apc+=rr.Apply(replicator)?1:0;
                }
                transaction.Commit();
            }
            return apc;
        }
    }
}
