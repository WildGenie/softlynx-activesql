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

    public class ReplicaManager:IDisposable
    {
        private Hashtable CMDS = new Hashtable();

        internal class CmdSet:IDisposable
        {
            internal DbCommand CleanReplicaCmd = null;
            internal DbCommand ExistReplicaCmd = null;
            internal DbCommand ConflictReplicaCmd = null;
            internal CmdSet(RecordManager Manager)
            {

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
        [TableVersion(1, TableAction.Recreate)]
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

        private Hashtable DBIDS = new Hashtable();
        private Hashtable RPIDS = new Hashtable();

        public int ReplicaBufferLimit = 1024 * 64;

        private Guid GetHashID(Hashtable ht, Guid gid, RecordManager Manager, bool generatenew)
        {
            ReplicaPeer rp = (ReplicaPeer)ht[Manager];
            if (rp == null)
            {
                rp = new ReplicaPeer();
                rp.ID = gid;
                Manager.Read(rp);
                ht[Manager] = rp;
            }
            if ((rp.ReplicaID == Guid.Empty) && generatenew)
            {
                rp.ReplicaID = Guid.NewGuid();
                Manager.Write(rp);
            }
            return rp.ReplicaID;
        }

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

        public Guid DBSelfGuid(RecordManager Manager)
        {
            return GetHashID(DBIDS,ReplicaPeer.ID_DbInstance, Manager,true);
        }

        public Guid DBSelfGuid() { return DBSelfGuid(RecordManager.Default); }


        public Guid DBSnapshot(RecordManager Manager)
        {
            return GetHashID(RPIDS, ReplicaPeer.ID_Snapshot, Manager, false);
        }

        public Guid DBSnapshot() { return DBSnapshot(RecordManager.Default); }

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
                MemoryStream ms = new MemoryStream(buf);
                XmlReader xr = XmlReader.Create(ms);
                xr.ReadStartElement();
                while (logserializer.CanDeserialize(xr))
                {
                    Object o = null;
                    o=logserializer.Deserialize(xr);
                    if (o is ReplicaLog)
                    {
                        cnt+=ApplyOperation(Manager, o as ReplicaLog);
                    }
                };
                t.Commit();
            }
            return cnt;   
        }

        public int ApplyReplicaBuffer(byte[] buf)
        {
           return ApplyReplicaBuffer(RecordManager.Default, buf);
        }


        XmlSerializer logserializer = new XmlSerializer(typeof(ReplicaLog));
        XmlWriterSettings serializer_settings = new XmlWriterSettings();
        XmlSerializerNamespaces serializer_ns = new XmlSerializerNamespaces();

        public ReplicaManager()
        {
            serializer_settings.CloseOutput = false;
            serializer_settings.NewLineChars = "";
            serializer_settings.NewLineHandling = NewLineHandling.None;
            serializer_settings.NewLineOnAttributes = false;
            serializer_settings.OmitXmlDeclaration = true;
            serializer_settings.Indent = false;
            serializer_ns.Add("", "");
        }


        public Hashtable ExcludeAuthor = new Hashtable();
        public byte[] BuildReplicaBuffer(RecordManager Manager,ref long lastknownid)
        {
            long logcnt = 0;
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms, serializer_settings);
            xw.WriteStartElement("ReplicaBuffer");
            foreach (ReplicaLog l in RecordIterator.Enum<ReplicaLog>(Manager, Manager.WhereExpression("SeqNO", ">") + " and " + Manager.WhereEqual("Actual"), Manager.AsFieldName("SeqNO"), "SeqNO", lastknownid,"Actual",true))
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
            CleanReplicaCmd.Parameters[0].Value = l.ObjectID;
            CleanReplicaCmd.ExecuteNonQuery();
            Manager.Write(l);
        }

        private string SerializeObject(InTable aro, object obj, ReplicaLog.Operation operation)
        {
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms,serializer_settings);
            InField[] fld = (operation == ReplicaLog.Operation.Write) ? aro.fields : aro.primary_fields;
            xw.WriteStartDocument();
            xw.WriteStartElement(aro.Name);
            foreach (InField f in fld)
            {
                xw.WriteStartElement(f.Name);
                xw.WriteValue(f.GetValue(obj));
                xw.WriteEndElement();
            }
            xw.WriteEndElement();
            xw.WriteEndDocument();
            xw.Close();
            string data = ValueFormatter.XmlStrFromBuffer(ms.ToArray(),3);
            return data;
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
                MemoryStream ms = new MemoryStream();
                TextWriter tw = new StreamWriter(ms);
                tw.Write(log.ObjectValue);
                tw.Flush();
                ms.Seek(0, SeekOrigin.Begin);
                Object instance = Activator.CreateInstance(rt);
                if (instance is IRecordManagerDriven)
                    (instance as IRecordManagerDriven).Manager = Manager;
                XmlReader xr = XmlReader.Create(ms);
                xr.ReadStartElement();
                while (!xr.EOF)
               {
                   if (xr.IsStartElement())
                   {
                       string fname = xr.Name;
                       InField f = it.Field(fname);
                       if (f != null)
                       {
                           xr.MoveToContent();
                           string fvalue = xr.ReadElementString();
                           f.SetValue(instance, fvalue);
                       }
                   }
                   else xr.Read();
                }
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
                            : Manager.Write(instance);
                    }
            }
            return 0;
        }
    }
}