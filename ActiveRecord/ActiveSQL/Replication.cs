using System;
using System.Collections;
using System.Threading;
using System.Text;
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
    public class ReplicaManager
    {

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
        public Guid ReplicaID
        {
            get { return _ReplicaID; }
            set { _ReplicaID = value; }
        }

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

    }

        Hashtable DBIDS = new Hashtable();
        Hashtable RPIDS = new Hashtable();
        Hashtable CMDS = new Hashtable();

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

        public Guid DBSelfGuid(RecordManager Manager)
        {
            return GetHashID(DBIDS,ReplicaPeer.ID_DbInstance, Manager,true);
        }

        public Guid DBSnapshot(RecordManager Manager)
        {
            return GetHashID(RPIDS, ReplicaPeer.ID_Snapshot, Manager, false);
        }


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

        private void LogOperation(RecordManager Manager, object obj, ReplicaLog.Operation operation)
        {
            InTable aro = Manager.ActiveRecordInfo(obj.GetType());
            if (!aro.with_replica) return;
            ReplicaLog l = new ReplicaLog();
            l.ID = Guid.NewGuid();
            l.AutorID = DBSelfGuid(Manager);
            l.ObjectName = obj.GetType().Name;
            l.ObjectValue=SerializeObject(aro, obj,operation);
            l.ObjectOperation = operation;
            DbCommand RemoveReplicaCmd = (DbCommand)CMDS[Manager];
            if (RemoveReplicaCmd == null)
            {
                RemoveReplicaCmd = Manager.CreateCommand(
                    "delete from " + Manager.AsFieldName(typeof(ReplicaLog).Name) +
                    "where " + Manager.AsFieldName("ObjectName") + "=" + Manager.AsFieldParam("ObjectName"),
                    "ObjectName", l.ObjectName);
                CMDS[Manager] = RemoveReplicaCmd;
            }
            else
            {
                RemoveReplicaCmd.Parameters[0].Value = l.ObjectName;
            }
            RemoveReplicaCmd.ExecuteNonQuery();
            Manager.Write(l);
        }

        XmlSerializer serializer = new XmlSerializer(typeof(ArrayList),"ActiveRecord");

        private string SerializeObject(InTable aro, object obj, ReplicaLog.Operation operation)
        {
            ArrayList objs=new ArrayList();
            InField[] fld = (operation == ReplicaLog.Operation.Write) ? aro.fields : aro.primary_fields;
            foreach (InField f in fld) {
                object value=f.prop.GetValue(obj, null);
                objs.Add(f.Name);
                objs.Add(value);
            }
            MemoryStream ms=new MemoryStream();
            serializer.Serialize(ms, objs);
            ms.Seek(0, SeekOrigin.Begin);
            TextReader tr = new StreamReader(ms);
            string data=tr.ReadToEnd();
            return data;
        }

        private int ApplyOperation(RecordManager Manager,ReplicaLog log)
        {
            Type rt = Manager.GetTypeFromTableName(log.ObjectName);
            InTable it = rt==null?null:Manager.ActiveRecordInfo(rt,false);
            if (it!=null)
            {
                ArrayList objs = new ArrayList();
                MemoryStream ms = new MemoryStream();
                TextWriter tw = new StreamWriter(ms);
                tw.Write(log.ObjectValue);
                tw.Flush();
                ms.Seek(0, SeekOrigin.Begin);
                object dobj=serializer.Deserialize(ms);
                if (dobj is ArrayList)
                {
                    objs = (ArrayList)dobj;
                    Hashtable vals=new Hashtable();
                    while (objs.Count > 1)
                    {
                        vals[objs[0]] = objs[1];
                        objs.RemoveRange(0, 2);
                    }
                    if (vals.Count>0) {
                    Object instance = Activator.CreateInstance(rt);

                    if (instance is IRecordManagerDriven)
                        (instance as IRecordManagerDriven).Manager = Manager;

                    foreach (InField f in it.fields)
                    {
                        object value = vals[f.Name];
                        if (value != null)
                            f.prop.SetValue(instance, value, null);
                    }
                        return (log.ObjectOperation==ReplicaLog.Operation.Delete)
                            ?Manager.Delete(instance)
                            :Manager.Write(instance);
                    }
                }
            }
            return 0;
        }
    }
}