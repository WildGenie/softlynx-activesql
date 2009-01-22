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
    public  class ReplicaPeers:IDObject
    {
        Int64 _SeqNO = 0;
        Int64 SeqNO
        {
            get { return _SeqNO; }
            set { _SeqNO=value; }
        }

        DateTime _LastUpdate = DateTime.Now;
        DateTime LastUpdate
        {
            get { return _LastUpdate; }
            set { _LastUpdate = value; }
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

        [InTable]
        public class InstanceID:IDObject
        {
            public static Guid SelfID=new Guid("{11942243-F7A6-47b9-9416-5C1BA978138E}");
            private Guid _value = Guid.Empty;
            public Guid Value
            {
                get { return _value; }
                set { _value = value; }
            }
        }

        Hashtable IDS = new Hashtable();
        Hashtable CMDS = new Hashtable();

        public Guid SelfGuid(RecordManager Manager)
        {
            Guid res = Guid.Empty;
            object o = IDS[Manager];
            if (o == null)
            {
                InstanceID II = new InstanceID();
                II.ID = InstanceID.SelfID;
                if (Manager.Read(II))
                {
                    res = II.Value;
                }
                else
                {
                    res = Guid.NewGuid();
                    II.Value = res;
                    Manager.Write(II);
                }
                IDS[Manager] = res;
            }
            else res = (Guid)o;
            return res;
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
                if (Manager.ActiveRecordInfo(typeof(InstanceID),false) == null)
                    Manager.TryToRegisterAsActiveRecord(typeof(InstanceID));

                Guid MyID = SelfGuid(Manager);

                if (Manager.ActiveRecordInfo(typeof(ReplicaLog), false) == null)
                    Manager.TryToRegisterAsActiveRecord(typeof(ReplicaLog));

                if (Manager.ActiveRecordInfo(typeof(ReplicaPeers), false) == null)
                    Manager.TryToRegisterAsActiveRecord(typeof(ReplicaPeers));


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
            l.AutorID = SelfGuid(Manager);
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
    }
}