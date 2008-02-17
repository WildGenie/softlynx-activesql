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

namespace Softlynx.SQLiteDataset.Replication
{
    /// <summary>
    /// Порция данных о репликах
    /// </summary>
    [Serializable]
    public class ReplicaPortion
    {
        ArrayList replicaset = new ArrayList();
        
        [XmlArray]
        public ReplicaRecord[] ReplicaSet
        {
            get { return (ReplicaRecord[])replicaset.ToArray(typeof(ReplicaRecord)); }
            set { replicaset.Clear();replicaset.AddRange(value); }
        }


        /// <summary>
        /// Заполняет ReplicaSet значениями реплик начиная с номера
        /// следующего за LastKnownSeqNo.
        /// Максимальное количество записей регламентируется replicator.MaxPortionSize.
        /// </summary>
        /// <param name="replicator">Родительский объект репликации</param>
        /// <param name="LastKnownSeqNo">Последний известный seqno реплики</param>
        public void RequestLog(SQLiteReplicator replicator, ref Int64 LastKnownSeqNo)
        {
            using (DbTransaction transaction = replicator.MasterDB.BeginTransaction())
            {
                try
                {
                    using (DbCommand cmd = replicator.MasterDB.CreateCommand())
                    {
                        cmd.CommandText = "select * from replica_log where seqno>@seqno";
                        cmd.Parameters.Add(new SQLiteParameter("@seqno", LastKnownSeqNo));
                        replicaset.Clear();
                        using (DbDataReader reader = cmd.ExecuteReader())
                        {
                            while ((replicaset.Count < replicator.MaxPortionSize) && reader.Read())
                            {
                                ReplicaRecord rr = new ReplicaRecord(replicator, reader);
                                LastKnownSeqNo = rr.SeqNo;
                                if (replicator.IgnoreTable.ContainsKey(rr.TableName)) continue;
                                if (replicator.IgnoreAuthor.ContainsKey(rr.Author)) continue;
                                rr.LoadReplicaValues(replicator);
                                replicaset.Add(rr);
                            };
                            reader.Close();
                        }
                    }
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
        }
        /// <summary>
        /// Посредством родительского объекта replicator применяет действия указынные в репликах 
        /// </summary>
        /// <param name="replicator">Родительский объект репликации</param>
        /// <returns>Число успешно выполненных реплик</returns>
        public int ApplyLog(SQLiteReplicator replicator)
        {
            if (ReplicaSet.Length <= 0) return 0;
            int apc = 0;
                using (DbTransaction transaction = replicator.MasterDB.BeginTransaction())
                {
                    try
                    {
                        foreach (ReplicaRecord rr in ReplicaSet)
                        {
                            apc += rr.Apply(replicator) ? 1 : 0;
                        }
                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            return apc;
        }
    }
}
