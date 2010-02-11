using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.IO;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using System.Reflection;

namespace Softlynx.ActiveSQL.Replication
{

    public class SnapshotRequiredException:Exception 
    {
        public SnapshotRequiredException(string message):base(message){}
    }

    public class ReplicaManager:PropertySet,IDisposable
    {
        public class Property
        {
            static public PropType DatabaseObject = new PropType<ReplicaPeer>("Database Identifier object");
            static public PropType SnapshotObject = new PropType<ReplicaPeer>("Snapshot Identifier object");
        }

        private Hashtable CMDS = new Hashtable();

        internal class CmdSet:IDisposable
        {
            internal DbCommand DeleteReplicaCmd = null;
            internal DbCommand CleanReplicaCmd = null;
            internal DbCommand ExistReplicaCmd = null;
            internal DbCommand ConflictReplicaCmd = null;
            internal CmdSet(RecordManager Manager)
            {
                DeleteReplicaCmd = Manager.CreateCommand(
                "delete from " + Manager.AsFieldName(typeof(ReplicaLog).Name) +
                " where " + Manager.WhereExpression("SeqNO", "<="),
                "SeqNO", 0);

                CleanReplicaCmd = Manager.CreateCommand(
                   "update " + Manager.AsFieldName(typeof(ReplicaLog).Name) +
                   " set " + Manager.WhereEqual("Actual") +
                   " where " + Manager.WhereEqual("ObjectID"),
                   "ObjectID", Guid.Empty,
                   "Actual", false);

                ExistReplicaCmd = Manager.CreateCommand(
                   "select 1 from " + Manager.AsFieldName(typeof(ReplicaLog).Name) +
                   "where " + Manager.WhereEqual("ID"),
                   "ID", Guid.Empty);

                ConflictReplicaCmd = Manager.CreateCommand(
                "select 1 from " + Manager.AsFieldName(typeof(ReplicaLog).Name) +
                " where " + Manager.WhereEqual("ObjectID")+
                " and " + Manager.WhereExpression("Created",">="),
                "ObjectID", Guid.Empty,
                "Created", DateTime.Now
                );

            }

            public void Dispose()
            {
                CleanReplicaCmd.Dispose();
                ExistReplicaCmd.Dispose();
                ConflictReplicaCmd.Dispose();
                DeleteReplicaCmd.Dispose();

            }

        }

        private CmdSet Commands(RecordManager Manager)
        {
            CmdSet res = (CmdSet)CMDS[Manager];
            if (res == null)
            {
                res = new CmdSet(Manager);
                CMDS[Manager] = res;
            }
            return res;
        }

        public void Dispose()
        {
            RecordManager m = RecordManager.Default;
            CmdSet res = (CmdSet)CMDS[m];
            if (res != null) res.Dispose();
            CMDS.Remove(m);
        }
    
        public delegate void ApplyReplicaEvent(ReplicaManager.ReplicaLog log, RecordManager manager);

    public event ApplyReplicaEvent OnApplyReplica=null;

    [InTable]
    public class ReplicaPeer:IDObject
    {
        public static Guid ID_DbInstance = new Guid("{11942243-F7A6-47b9-9416-5C1BA978138E}");
        public static Guid ID_Snapshot = new Guid("{EF30C2E9-E035-4f98-A76D-AFA5075AC66A}");

        Int64 _SeqNO = 0;
        public Int64 SeqNO
        {
            get { return _SeqNO; }
            set { _SeqNO=value; }
        }

        DateTime _LastUpdate = DateTime.Now;
        public DateTime LastUpdate
        {
            get { return _LastUpdate; }
            set { _LastUpdate = value; }
        }

        private Guid _ReplicaID = Guid.Empty;
        [Indexed]
        public Guid ReplicaID
        {
            get { return _ReplicaID; }
            set { _ReplicaID = value; }
        }
    }
        private int SkipReplication = 0;

        public class ReplicaContext : IDisposable
        {
            ReplicaManager manager=null;

            internal ReplicaContext(ReplicaManager Manager)
            {
                manager = Manager;
                manager.SkipReplication++;

            }
            public void Dispose()
            {
                manager.SkipReplication--;
            }
        }

        public ReplicaContext DisableLogger
        {
            get { return new ReplicaContext(this); }
        }

        [InTable]
        public class ReplicaLog : IIDObject
    {
        public enum Operation {Write,Delete};

        private Guid _ID;
        [PrimaryKey(false)]
        [Indexed]
        public Guid ID
        {
            get { return _ID; }
            set { _ID = value; }
        }

            public bool HasID
            {
                get { return !Guid.Empty.Equals(ID); }
            }



        private Int64 _SeqNO = 0;
        [Autoincrement]
        public Int64 SeqNO
        {
            get { return _SeqNO; }
            set { _SeqNO = value; }
        }

        DateTime _Created = DateTime.Now;
        public DateTime Created
        {
            get { return _Created; }
            set { _Created = value; }
        }

        Guid _AutorID = Guid.Empty;
        [Indexed]
        public Guid AutorID
        {
            get { return _AutorID; }
            set { _AutorID = value; }
        }

        private Guid _ObjectID = Guid.Empty;
        [Indexed]
        public Guid ObjectID
        {
            get { return _ObjectID; }
            set { _ObjectID = value; }
        }


        String _ObjectName = string.Empty;
        [Indexed]
        [InField(Size=512)]
        public String ObjectName
        {
            get { return _ObjectName; }
            set { _ObjectName = value; }
        }

        String _ObjectValue = null;
        public String ObjectValue
        {
            get { return _ObjectValue; }
            set { _ObjectValue = value; }
        }

        Operation _Operation = Operation.Write;
        [Indexed]
        public Operation ObjectOperation
        {
            get { return _Operation; }
            set { _Operation = value; }
        }

        private bool _Actual = true;
        [Indexed]
        public bool Actual
        {
            get { return _Actual; }
            set { _Actual = value; }
        }

        private bool _PotentialConflict = false;
        [Indexed]
        public bool PotentialConflict
        {
            get { return _PotentialConflict; }
            set { _PotentialConflict = value; }
        }
    }


        public int ReplicaBufferLimit = 1024 * 64;


        public ReplicaPeer Peer(RecordManager manager,Guid ID)
        {
            ReplicaPeer peer = new ReplicaPeer();
            peer.ID = ID;
            manager.Read(peer);
            return peer;
        }

        public ReplicaPeer Peer(Guid ID)
        {
            return Peer(RecordManager.Default, ID);
        }

        /// <summary>
        /// Returns cached ReplicaPeer record for this dataabase instance
        /// </summary>
        /// <param name="Manager">Assiciated record manager</param>
        /// <returns>ReplicaPeer record either existing or created</returns>
        public ReplicaPeer DbObject(RecordManager Manager)
        {
            return GetValue<ReplicaPeer>(Property.DatabaseObject, new DefaultValueDelegate<ReplicaPeer>(
             delegate
             {
                 ReplicaPeer rp = new ReplicaPeer();
                 rp.ID = ReplicaPeer.ID_DbInstance;
                 if (!Manager.Read(rp) || Guid.Empty.Equals(rp.ReplicaID))
                 {
                     rp.ReplicaID = Guid.NewGuid();
                     Manager.Write(rp);
                 }
                 return rp;
             }));
        }

        /// <summary>
        /// Returns cached ReplicaPeer record for this snapshot instance
        /// </summary>
        /// <param name="Manager">Assiciated record manager</param>
        /// <returns>ReplicaPeer record either existing</returns>
        public ReplicaPeer SnapshotObject(RecordManager Manager)
        {
            return GetValue<ReplicaPeer>(Property.SnapshotObject, new DefaultValueDelegate<ReplicaPeer>(
             delegate
             {
                 ReplicaPeer rp = new ReplicaPeer();
                 rp.ID = ReplicaPeer.ID_Snapshot;
                 Manager.Read(rp);
                 return rp;
             }));
        }


        /// <summary>
        /// Database instance identity
        /// </summary>
        /// <param name="Manager">Record amanager for requested database</param>
        /// <returns>GUID for the database</returns>
        public Guid DBSelfGuid(RecordManager Manager)
        {
            return DbObject(Manager).ReplicaID;
        }
        
        /// <summary>
        /// Database instalnce identity for RecordManager.Default
        /// </summary>
        /// <returns>GUID for the database</returns>
        public Guid DBSelfGuid() { return DBSelfGuid(RecordManager.Default); }

        /// <summary>
        /// Snapshot instance identity
        /// </summary>
        /// <param name="Manager">Record amanager for requested database</param>
        /// <returns>GUID for the database</returns>
        public Guid DBSnapshotGuid(RecordManager Manager)
        {
            return SnapshotObject(Manager).ReplicaID;
        }

        /// <summary>
        /// Returns last SeqNo value that stored in a snapshot 
        /// and not avalable any more via ReplicaLog
        /// </summary>
        /// <param name="Manager">Associated record manager</param>
        /// <returns>SeqNO value</returns>
        public long SnapshotSeqNO(RecordManager Manager)
        {
            return SnapshotObject(Manager).SeqNO;
        }


        public Guid DBSnapshotGuid() { return DBSnapshotGuid(RecordManager.Default); }

        public void RegisterWithRecordManager()
        {
            RegisterWithRecordManager(ActiveSQL.RecordManager.Default);
        }

        public void RegisterWithRecordManager(RecordManager Manager)
        {
            bool hasreplica = false;
            foreach (InTable t in Manager.RegisteredTypes)
            {
                if (t.with_replica)
                {
                    hasreplica = true;
                    break;
                }
            }
            if (hasreplica)
            using (ManagerTransaction mt = Manager.BeginTransaction())
            {

                if (Manager.ActiveRecordInfo(typeof(ReplicaLog), false) == null)
                    Manager.TryToRegisterAsActiveRecord(typeof(ReplicaLog));

                if (Manager.ActiveRecordInfo(typeof(ReplicaPeer), false) == null)
                    Manager.TryToRegisterAsActiveRecord(typeof(ReplicaPeer));

                Guid MyID = DBSelfGuid(Manager);

                Manager.OnRecordWritten += new RecordOperation(Manager_OnRecordWritten);
                Manager.OnRecordDeleted += new RecordOperation(Manager_OnRecordDeleted);
                mt.Commit();
            }
        }

        void Manager_OnRecordDeleted(RecordManager Manager, object obj)
        {
            LogOperation(Manager, obj, ReplicaLog.Operation.Delete);    
        }

        void Manager_OnRecordWritten(RecordManager Manager, object obj)
        {
            LogOperation(Manager, obj, ReplicaLog.Operation.Write);
        }

       
        public int ApplyReplicaBuffer(RecordManager Manager, byte[] buf)
        {
            int cnt = 0;
            using (ManagerTransaction t = Manager.BeginTransaction())
            {
                try
                {
                    MemoryStream ms = new MemoryStream(buf);
                    XmlReader xr = XmlReader.Create(ms);
                    xr.ReadStartElement();
                    while (logserializer.CanDeserialize(xr))
                    {
                        Object o = null;
                        o = logserializer.Deserialize(xr);
                        if (o is ReplicaLog)
                        {
                            cnt += ApplyOperation(Manager, o as ReplicaLog);
                        }
                    };
                } catch {
                    t.Rollback();
                    throw;
                }
                t.Commit();
            }
            return cnt;   
        }

        public int ApplyReplicaBuffer(byte[] buf)
        {
           return ApplyReplicaBuffer(RecordManager.Default, buf);
        }


        XmlSerializer logserializer = new XmlSerializer(typeof(ReplicaLog));
        XmlSerializerNamespaces serializer_ns = new XmlSerializerNamespaces();


        public ReplicaManager()
        {
            serializer_ns.Add("", "");
        }


        public Hashtable ExcludeAuthor = new Hashtable();
        public byte[] BuildReplicaBuffer(RecordManager Manager,ref long lastknownid)
        {
            if (lastknownid < SnapshotSeqNO(Manager))
                throw new SnapshotRequiredException("Fresh snapshot required to proceed");
            long logcnt = 0;
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms, ValueFormatter.SerializerSettings);
            xw.WriteStartElement("ReplicaBuffer");
            foreach (ReplicaLog l in RecordIterator.Enum<ReplicaLog>(Manager,
                Where.GT("SeqNO",lastknownid),
                Where.EQ("Actual",true)))
            {
                lastknownid = l.SeqNO;
                if (!ExcludeAuthor.ContainsKey(l.AutorID))
                {
                    logserializer.Serialize(xw,l,serializer_ns);
                    logcnt++;
                }
                xw.Flush();
                if (ms.Length > ReplicaBufferLimit) break;
            }
            xw.WriteEndElement();
            xw.Close();
            return  logcnt> 0 ? ms.ToArray() : null;
        }

        public byte[] BuildReplicaBuffer(ref long lastknownid)
        {
            return BuildReplicaBuffer(RecordManager.Default, ref lastknownid);
        }

        private void DeleteLogOperations(RecordManager Manager, long MaxSeqNo)
        {
            DbCommand DeleteReplicaCmd = Commands(Manager).DeleteReplicaCmd;
            DeleteReplicaCmd.Transaction = Manager.transaction;
            DeleteReplicaCmd.Parameters[0].Value = MaxSeqNo;
            DeleteReplicaCmd.ExecuteNonQuery();
        }

        private void LogOperation(RecordManager Manager, object obj, ReplicaLog.Operation operation)
        {
            if (SkipReplication > 0) 
                return;
            InTable aro = Manager.ActiveRecordInfo(obj.GetType());
            if (!aro.with_replica) return;
            ReplicaLog l = new ReplicaLog();
            l.ID = Guid.NewGuid();
            l.AutorID = DBSelfGuid(Manager);
            l.ObjectName = obj.GetType().Name;
            l.ObjectID = (Guid)aro.PKEYValue(obj);
            l.ObjectValue=SerializeObject(aro, obj,operation);
            l.ObjectOperation = operation;
            l.Actual = true;
            DbCommand CleanReplicaCmd = Commands(Manager).CleanReplicaCmd;
            CleanReplicaCmd.Transaction = Manager.transaction;
            CleanReplicaCmd.Parameters[0].Value = l.ObjectID;
            CleanReplicaCmd.ExecuteNonQuery();
            Manager.Write(l);
        }

        /// <summary>
        /// Serialize the object with RecordManager.Default instance
        /// </summary>
        /// <param name="obj">ActiveRecord enabled class instance</param>
        /// <returns>Compact XML representation</returns>
        public static string SerializeObject(object obj)
        {
            return SerializeObject(RecordManager.Default, obj);
        }

        /// <summary>
        /// Serialize the object with specified RecordManager
        /// </summary>
        /// <param name="Manager">Associated RecordManager instance</param>
        /// <param name="obj">ActiveRecord enabled class instance</param>
        /// <returns>Compact XML object representation</returns>
        public static string SerializeObject(RecordManager Manager, object obj)
        {
            return SerializeObject(Manager, obj, ReplicaLog.Operation.Write);
        }
        /// <summary>
        /// Serialize the object with RecordManager.Default instance
        /// </summary>
        /// <param name="obj">ActiveRecord enabled class instance</param>
        /// <param name="operation">In case of ReplicaLog.Operation.Delete will serialize only the primary key fields</param>
        /// <returns>Compact XML object representation</returns>
        public static string SerializeObject(object obj, ReplicaLog.Operation operation)
        {
            return SerializeObject(RecordManager.Default.ActiveRecordInfo(obj.GetType()), obj, operation);
        }
        
        /// <summary>
        /// Serialize the object with specified RecordManager
        /// </summary>
        /// <param name="Manager">Associated RecordManager instance</param>
        /// <param name="obj">ActiveRecord enabled class instance</param>
        /// <param name="operation">In case of ReplicaLog.Operation.Delete will serialize only the primary key fields</param>
        /// <returns>Compact XML object representation</returns>
        public static string SerializeObject(RecordManager Manager, object obj, ReplicaLog.Operation operation)
        {
            return SerializeObject(Manager.ActiveRecordInfo(obj.GetType()),obj, operation);
        }

        private static string SerializeObject(InTable ActiveRecordInfo, object obj, ReplicaLog.Operation operation)
        {
            StringBuilder sb = new StringBuilder();
            XmlWriter xw = XmlWriter.Create(sb, ValueFormatter.SerializerSettings);
                InField[] fld = (operation == ReplicaLog.Operation.Write) ? ActiveRecordInfo.fields : ActiveRecordInfo.primary_fields;
                xw.WriteStartDocument();
                xw.WriteStartElement(ActiveRecordInfo.Name);
                foreach (InField f in fld)
                {
                    xw.WriteStartElement(f.Name);
                    xw.WriteValue(f.GetValue(obj));
                    xw.WriteEndElement();
                }
                xw.WriteEndElement();
                xw.WriteEndDocument();
                xw.Close();
            return sb.ToString();
        }

        public static object DeserializeObject(string data)
        {
            return DeserializeObject(RecordManager.Default, data);
        }

        public static object DeserializeObject(RecordManager Manager,string data)
        {
            return DeserializeObject(Manager,null, data);
        }

        public static T DeserializeObjectAs<T>(string data)
        {
            return DeserializeObjectAs<T>(RecordManager.Default,data);
        }

        public static object DeserializeObjectAs(Type t, string data)
        {
            return DeserializeObjectAs(t, data);
        }

        public static T DeserializeObjectAs<T>(RecordManager Manager, string data)
        {
            return (T)DeserializeObjectAs(Manager,typeof(T), data);
        }

        public static object DeserializeObjectAs(RecordManager Manager, Type t, string data)
        {
            InTable it = Manager.ActiveRecordInfo(t, false);
            return DeserializeObject(Manager, it, data);
        }

        private static object DeserializeObject(RecordManager Manager,InTable ActiveRecordInfo, string data)
        {
            XmlReader xr = XmlReader.Create(new StringReader(data));
            xr.Read();
            if (ActiveRecordInfo == null)
            {
                Type rt = Manager.GetTypeFromTableName(xr.Name);
                if (rt!=null)
                    ActiveRecordInfo=Manager.ActiveRecordInfo(rt, false);
                if (ActiveRecordInfo == null)
                    throw new ApplicationException("Can't find any ActiveRecord class named " + xr.Name);
            }

            Object instance = Activator.CreateInstance(ActiveRecordInfo.basetype);
            if (instance is IRecordManagerDriven)
                (instance as IRecordManagerDriven).RecordManager = Manager;

            xr.ReadStartElement();

            while (!xr.EOF)
            {
                if (xr.IsStartElement())
                {
                    string fname = xr.Name;
                    xr.MoveToContent();
                    string fvalue = xr.ReadElementString();
                    InField f = ActiveRecordInfo.Field(fname);
                    if (f != null)
                        f.SetValue(instance, fvalue);
                }
                else xr.Read();
            }
            return instance;
        }


        public bool IsReplicaExists(RecordManager Manager,Guid ReplicaGuid)
        {
            DbCommand TestReplicaCmd = Commands(Manager).ExistReplicaCmd;
            TestReplicaCmd.Parameters[0].Value = ReplicaGuid;
            return (TestReplicaCmd.ExecuteScalar() != null) ;
        }

        public bool IsReplicaExists(Guid ReplicaGuid)
        {
            return IsReplicaExists(RecordManager.Default, ReplicaGuid);
        }

        private int ApplyOperation(RecordManager Manager,ReplicaLog log)
        {

            if (IsReplicaExists(Manager,log.ID)) return 0;

            Type rt = Manager.GetTypeFromTableName(log.ObjectName);
            InTable it = rt==null?null:Manager.ActiveRecordInfo(rt,false);
            if (it!=null)
            {
                 Object instance =  DeserializeObject(Manager,it, log.ObjectValue);
                    using (DisableLogger)
                    {
                        DbCommand TestConflict = Commands(Manager).ConflictReplicaCmd;
                        TestConflict.Parameters[0].Value = log.ObjectID;
                        TestConflict.Parameters[1].Value = log.Created;
                        log.PotentialConflict = (TestConflict.ExecuteScalar() != null);
                        if (OnApplyReplica != null)
                            OnApplyReplica(log, Manager);
                        if (log.PotentialConflict) return 0;
                        return (log.ObjectOperation == ReplicaLog.Operation.Delete)
                            ? Manager.Delete(instance)
                            : Manager.Write(instance,true);
                    }
            }
            return 0;
        }

        /// <summary>
        /// Creates a snapshot for source record managare
        /// and writes all replicated objects into destination provider.
        /// New Snapshot GUID is generated internaly.
        /// If there is no replicated objects then no snapshot will be created.
        /// </summary>
        /// <param name="RM">Soirce record manager</param>
        /// <param name="rplprov">Destination provider</param>
        public void BuildSnapshot(RecordManager RM, ProviderSpecifics rplprov)
        {
            BuildSnapshot(RM, rplprov, Guid.NewGuid());
        }

        /// <summary>
        /// Creates a snapshot for source record manager
        /// and writes all replicated objects into destination provider.
        /// If there is no replicated objects then no snapshot will be created.
        /// 
        /// Source RM starts to hold a snapshot object thet defines the maximum 
        /// value for SeqNO that is stored within created snapshot and no more avaliable 
        /// via BuildReplicaBuffer/ApplyReplicaBuffer framework cause they are cleared out.
        /// 
        /// Latest SeqNO stored in snapshot avaliable with SnapshotSeqNO(RM).
        /// 
        /// Any request to ReplicaManager with value less than 
        /// SnapshotSeqNO will produce an SnapshotRequiredException.
        /// </summary>
        /// <param name="RM">Soirce record manager</param>
        /// <param name="rplprov">Destination provider</param>
        /// <param name="SnapshotID">Guid identity for a new created snapshot</param>
        public void BuildSnapshot(RecordManager RM, ProviderSpecifics rplprov,Guid SnapshotID)
        {
            List<Type> dsttypes = new List<Type>();
            foreach (InTable t in RM.RegisteredTypes)
                if (t.with_replica) dsttypes.Add(t.basetype);

            if (dsttypes.Count == 0) return;

            using (ManagerTransaction trans = RM.BeginTransaction())
            {
                DeleteProperty(Property.DatabaseObject);
                DeleteProperty(Property.SnapshotObject);
                ReplicaPeer snapshot_record = SnapshotObject(RM);
                ReplicaPeer database_record = DbObject(RM);

                snapshot_record.LastUpdate = DateTime.Now;
                snapshot_record.ReplicaID = SnapshotID;
                long maxfoundseqno = snapshot_record.SeqNO;
                foreach (ReplicaLog rl in RecordIterator.Enum<ReplicaLog>(
                    RM,
                    Where.GT("SeqNO",maxfoundseqno),
                    Where.OrderBy("SeqNO", WhereCondition.Descendant),
                    Where.Limit(1)))
                    maxfoundseqno = rl.SeqNO;

                snapshot_record.SeqNO = maxfoundseqno;
                RM.Write(snapshot_record);

                using (RecordManager snapshot = new RecordManager(rplprov, dsttypes.ToArray()))
                {
                    using (ManagerTransaction snap_trans = snapshot.BeginTransaction())
                    {
                        ReplicaManager snapshotreplica = new ReplicaManager();
                        snapshotreplica.RegisterWithRecordManager(snapshot);

                        snapshot_record = (ReplicaPeer)snapshot_record.Clone();
                        snapshot_record.SeqNO = 0;
                        snapshot.Write(snapshot_record);

                        ReplicaPeer dstdb = Peer(snapshot, database_record.ReplicaID);
                        dstdb.SeqNO = maxfoundseqno;
                        snapshot.Write(dstdb);

                        database_record = (ReplicaPeer)database_record.Clone();
                        database_record.ReplicaID = Guid.Empty;
                        snapshot.Write(database_record);

                        using (ReplicaContext context = snapshotreplica.DisableLogger)
                        {
                            foreach (Type t in dsttypes)
                                foreach (Object o in RecordIterator.Enum(t, RM))
                                    snapshot.Write(o,true);
                            DeleteLogOperations(RM, maxfoundseqno);
                            trans.Commit();
                        }
                        snap_trans.Commit();
                    }
                }
            }
        }
    }
}
