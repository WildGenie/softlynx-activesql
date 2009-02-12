﻿using System;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.IO;
using Softlynx.RecordCache;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Serialization;


namespace Softlynx.ActiveSQL
{
    public class NotActiveRecordException : ApplicationException { 
        public NotActiveRecordException(string msg):base(msg){}
    };

    public interface IProviderSpecifics
    {
        DbParameter CreateParameter(string name, object value);
        DbParameter CreateParameter(string name, Type t);
        string GetSqlType(Type t);
        DbType GetDbType(Type t);
        string AsFieldName(string s);
        string AsFieldParam(string s);
        string AutoincrementStatement(string ColumnName);
        DbConnection Connection
        {
            get;
        }
        void ExtendConnectionString(string key, string value);
        string AdoptSelectCommand(string select, InField[] fields);
    }

    #region Common attributes
    public enum TableAction { None, RunSQL, Recreate };
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ExcludeFromTable : Attribute { }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class Indexed : Attribute { }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class Autoincrement : Attribute { }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class WithReplica : Attribute { }

    public class NamedAttribute : Attribute
    {
        private string _name = string.Empty;
        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class BeforeRecordManagerDelete : Attribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerDelete : Attribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class BeforeRecordManagerWrite : Attribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerWrite : Attribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerRead : Attribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class RecordManagerPostRegistration : Attribute { }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class PrimaryKey : NamedAttribute {
        internal bool _GenerateSQL_PK = true;
        public PrimaryKey() { }
        public PrimaryKey(bool GenerateSQL_PK) { _GenerateSQL_PK = GenerateSQL_PK; }
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetInsert : NamedAttribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetRemove : NamedAttribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class OnTableVersionChange : Attribute { }


    
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class TableVersion : NamedAttribute,IComparable<TableVersion>
    {
        private int _version;

        public int Version
        {
            get { return _version; }
            set { _version = value; }
        }

        private string _sql_code;
        private TableAction _table_action = TableAction.None;

        public string SQLCode
        {
            get { return _sql_code; }
            set { _sql_code = value; }
        }

        public TableAction Action
        {
            get { return _table_action; }
            set { _table_action = value; }
        }

        public TableVersion(int version, string sql_code)
        {
            Version = version;
            Action = TableAction.RunSQL;
            SQLCode = sql_code;
        }

        public TableVersion(int version, TableAction action)
        {
            Version = version;
            Action = action;
            SQLCode = string.Empty;
        }

        public TableVersion(int version, TableAction action, string sql_code)
        {
            Version = version;
            Action = action;
            SQLCode = sql_code;
        }

        public int CompareTo(TableVersion obj)
        {
            return Version.CompareTo(obj.Version);
        }

    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class InField : NamedAttribute
    {
        internal bool IsPrimary = false;
        internal bool GenerateSQL_PK = true;
        internal bool IsIndexed = false;
        internal bool IsAutoincrement = false;
        internal Type field_type = typeof(object);
        internal PropertyInfo _prop = null;
        XmlSerializer serializer = null;
        XmlWriterSettings ws = new XmlWriterSettings();

        internal PropertyInfo prop
        {
            get { return _prop; }
            set { 
             
                _prop=value;
                SetupSerialization();
            }
        }

        private void SetupSerialization()
        {
            serializer = null;

            if (prop.CanWrite) 
            {
                serializer = new XmlSerializer(_prop.PropertyType, new XmlRootAttribute("VALUE"));
                ws.CloseOutput = true;
                ws.NewLineChars = "";
                ws.NewLineHandling = NewLineHandling.None;
                ws.NewLineOnAttributes = false;
                ws.OmitXmlDeclaration = true;
                ws.Indent = false;
            };
                
        }

        internal string GetValue(object obj)
        {
            MemoryStream ms = new MemoryStream();
            object v = prop.GetValue(obj, null);
            XmlWriter xw = XmlWriter.Create(ms, ws);
            serializer.Serialize(xw, v);
            MemoryStream ms1 = new MemoryStream(ms.ToArray());
            XmlReader r = XmlReader.Create(ms1);
            string xml=r.ReadElementString();
            return xml;
        }

        internal void SetValue(object obj,string v)
        {
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms);
            xw.WriteElementString("VALUE", v);
            xw.Close();
            MemoryStream ms1 = new MemoryStream(ms.ToArray());
            object ov=serializer.Deserialize(ms1);
            prop.SetValue(obj, ov, null);
        }
        
        

        internal String CreateColumnStatement(RecordManager manager)
        {

            return  
                IsAutoincrement?
                String.Format(manager.Autoincrement(Name)):
                String.Format("{0} {1}", manager.AsFieldName(Name), manager.SqlType(this));
        }

        public Type FieldType
        {
            get { return field_type; }
        }
    }


    #endregion
    
    public delegate void RecordManagerEvent(object o);
    public delegate void RecordManagerWriteEvent(object o, ref bool Handled);
    public delegate void RecordSetEvent(object o, object recordset);

  
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class InTable : NamedAttribute,ICloneable

    {
        internal RecordManager manager = null;

        private DbCommand InsertCmd = null;
        private DbCommand UpdateCmd = null;
        private DbCommand DeleteCmd = null;
        private DbCommand FillCmd = null;
        internal bool with_replica = false;

        private InField[] _fields;
        private Hashtable hf = new Hashtable();
        internal InField[] fields
        {
            get { return _fields; }
            set { 
                _fields = value; 
                hf.Clear();
                foreach (InField f in _fields)
                {
                    hf[f.Name] = f;
                }
            }
        }

        internal InField Field(string name)
        {
            return (InField)hf[name];
        }

        internal InField[] primary_fields;
        internal Type basetype = default(Type);
        internal Hashtable foreign_keys = new Hashtable();
        public event RecordManagerEvent BeforeRecordManagerDelete = null;
        public event RecordManagerEvent AfterRecordManagerDelete = null;
        public event RecordManagerWriteEvent BeforeRecordManagerWrite = null;
        public event RecordManagerEvent AfterRecordManagerWrite = null;
        public event RecordManagerEvent AfterRecordManagerRead = null;
        public event RecordManagerEvent RecordManagerPostRegistration = null;
        public event RecordManagerEvent TableVersionChanged = null;
        public event RecordSetEvent RecordSetInsert = null;
        public event RecordSetEvent RecordSetRemove = null;

        public object Clone()
        {
            return this.MemberwiseClone();
        }

        internal object PKEYValue(object o)
        {
            return primary_fields[0].prop.GetValue(o, null);
        }

        internal virtual bool IsVirtual
        {
            get { return false; }
        }

        internal void SetPKEYValue(object o, object v)
        {
            primary_fields[0].prop.SetValue(o, v, null);
        }

        internal String ColumnsList(ICollection fromcolums, bool asparams, string separator)
        {
            int i = 0;
            String[] clist = new string[fromcolums.Count];
            foreach (Object c in fromcolums)
            {
                string cname = String.Empty;
                if (c is InField) cname = (c as InField).Name;
                if (cname == String.Empty) throw new Exception("Can't get column name from type " + fromcolums.GetType().ToString());
                if (asparams)
                    clist[i++] = manager.AsFieldParam(cname);
                else
                    clist[i++] = manager.AsFieldName(cname);
            };
            return String.Join(separator, clist);
        }

        internal String ColumnsList(ICollection fromcolums, bool asparams)
        {
            return ColumnsList(fromcolums, asparams, ",");
        }

        internal String ColumnsList(ICollection fromcolums)
        {
            return ColumnsList(fromcolums, false, ",");
        }

        internal String DropTableStatement()
        {
            return String.Format("DROP TABLE IF EXISTS {0};", manager.AsFieldName(Name));
        }

        internal String CreateTableStatement()
        {
            String s = String.Format("CREATE TABLE {0}(\n", manager.AsFieldName(Name));
            String cols = String.Empty;
            foreach (InField col in fields)
            {
                if (cols != String.Empty) cols += ",\n";
                cols += String.Format("{0}", col.CreateColumnStatement(manager));
            }
            s += cols;

            if (primary_fields.Length ==0)
            {
                throw new Exception(String.Format("Primary key not defined for table {0}", Name));
            };


            List<InField> PKf = new List<InField>();
            foreach (InField f in primary_fields)
            {
                if (f.IsPrimary && f.GenerateSQL_PK)
                    PKf.Add(f);
            }

            if (PKf.Count > 0)
            {
                string flags = string.Empty;
                s += String.Format(",\nPRIMARY KEY ({0}) {1}",
                    ColumnsList(PKf),
                    flags
                    );
            }
            s += String.Format("\n);\n");
            foreach (InField f in fields)
            {
                if (f.IsIndexed) s += string.Format("CREATE INDEX {0}_idx on {1}({2});\n", Name+"_"+f.Name, manager.AsFieldName(Name), manager.AsFieldName(f.Name));
            }
            return s;
        }

        string InsertCommandText()
        {
            List<InField> insertfields = new List<InField>();
            foreach (InField f in fields)
            {
                if (!f.IsAutoincrement)
                    insertfields.Add(f);
            }

            return String.Format("INSERT INTO {0}({1}) values ({2})",
                manager.AsFieldName(Name),
                ColumnsList(insertfields),
                ColumnsList(insertfields, true)
                );
        }

        string UpdateCommandText()
        {
            string pkeycolumns = string.Empty;
            string keyvalpairs = string.Empty;
            foreach (InField dc in fields)
            {
                if (keyvalpairs != String.Empty) keyvalpairs += ",";
                keyvalpairs += String.Format("{0}={1}", manager.AsFieldName(dc.Name), manager.AsFieldParam(dc.Name));
            };

            foreach (InField dc in primary_fields)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}={1}", manager.AsFieldName(dc.Name), manager.AsFieldParam(dc.Name));
            };
            if (pkeycolumns == String.Empty) throw new Exception(string.Format("Can't construct update quiery for type {0} until primary key is defined", Name));

            return String.Format("UPDATE {0} set {1} where ({2})",
                manager.AsFieldName(Name),
                keyvalpairs,
                pkeycolumns
                );
        }

        string DeleteCommandText()
        {
            string pkeycolumns = string.Empty;
            foreach (InField dc in primary_fields)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}={1}", manager.AsFieldName(dc.Name), manager.AsFieldParam(dc.Name));
            };
            if (pkeycolumns == String.Empty) throw new Exception("Can't delete until primary key defined");
            return String.Format("DELETE FROM {0} where ({1})",
                manager.AsFieldName(Name),
                pkeycolumns
                );
        }

        string FillCommandText()
        {
            string pkeycolumns = string.Empty;
            foreach (InField dc in primary_fields)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}={1}", manager.AsFieldName(dc.Name), manager.AsFieldParam(dc.Name));
            };
            return manager.AdoptSelectCommand(
             String.Format("SELECT {0} FROM {1} where ({2})",
                ColumnsList(fields),
                manager.AsFieldName(Name),
                pkeycolumns
                ),fields);
        }


        internal virtual void InitSqlMethods()
        {
            InsertCmd = manager.CreateCommand(InsertCommandText());
            UpdateCmd = manager.CreateCommand(UpdateCommandText());
            DeleteCmd = manager.CreateCommand(DeleteCommandText());
            FillCmd = manager.CreateCommand(FillCommandText());

           
            foreach (InField field in fields)
            {

                DbParameter prm = manager.CreateParameter(field);
                if (!field.IsAutoincrement)
                    InsertCmd.Parameters.Add(prm);
                UpdateCmd.Parameters.Add(prm);
            }

            foreach (InField field in primary_fields)
            {
                DbParameter prm = manager.CreateParameter(field);
                DeleteCmd.Parameters.Add(prm);
                FillCmd.Parameters.Add(prm);
            }
            

            InsertCmd.Prepare();
            UpdateCmd.Prepare();
            DeleteCmd.Prepare();
            FillCmd.Prepare();

        }
        
        internal virtual bool Read(object Record)
        {
            RecordManager.ReopenConnection(FillCmd);
            bool res = false;
            foreach (InField field in primary_fields)
            {
                FillCmd.Parameters[field.Name].Value = field.prop.GetValue(Record, null);
            }
            using (DbDataReader reader = FillCmd.ExecuteReader())
            {
                int i = 0;
                if (reader.Read())
                {
                    foreach (InField field in fields)
                    {
                        Object v = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        field.prop.SetValue(Record, v, null);
                        i++;
                    }
                    res = true;
                }
                reader.Close();
            }
            if (Record is IRecordManagerDriven)
                (Record as IRecordManagerDriven).Manager = manager;
            if (res && (AfterRecordManagerRead != null))
                AfterRecordManagerRead(Record);
            return res;
        }


        internal virtual int Update(object Record)
        {
            RecordManager.ReopenConnection(UpdateCmd);
            foreach (InField field in fields)
            {
                UpdateCmd.Parameters[field.Name].Value = field.prop.GetValue(Record, null);
            }
            return UpdateCmd.ExecuteNonQuery();
        }

        internal virtual int Insert(object Record)
        {
            RecordManager.ReopenConnection(InsertCmd);
            foreach (InField field in fields)
            {
                if (!field.IsAutoincrement)
                    InsertCmd.Parameters[field.Name].Value = field.prop.GetValue(Record, null);
            }
            return InsertCmd.ExecuteNonQuery();
        }

        internal virtual int Write(object Record)
        {
            using (ManagerTransaction t=manager.BeginTransaction())
            {
                if (Record is IRecordManagerDriven)
                    (Record as IRecordManagerDriven).Manager = manager;

                bool write_handled = false;
                int r = 0;
                if (BeforeRecordManagerWrite != null) BeforeRecordManagerWrite(Record, ref write_handled);
                if (!write_handled)
                {
                    r = Update(Record);
                    if (r == 0) r = Insert(Record);
                };
                if (AfterRecordManagerWrite != null) AfterRecordManagerWrite(Record);
                t.Commit();
                return r;
            }
        }

        internal void DoInsertToRecordSet(object o, object RecordSet)
        {
            if (RecordSetInsert != null)
                RecordSetInsert(o, RecordSet);
        }

        internal void DoRemoveFromRecordSet(object o, object RecordSet)
        {
            if (RecordSetRemove!=null)
                RecordSetRemove(o, RecordSet);
        }

        internal virtual int Delete(object Record)
        {
            RecordManager.ReopenConnection(DeleteCmd);
            int res = 0;
            using (ManagerTransaction transaction = manager.BeginTransaction())
            {
                if (Record is IRecordManagerDriven)
                    (Record as IRecordManagerDriven).Manager = manager;

                if (BeforeRecordManagerDelete != null) BeforeRecordManagerDelete(Record);
                int i = 0;
                foreach (InField field in primary_fields)
                {
                    DeleteCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
                }
                res = DeleteCmd.ExecuteNonQuery();
                if (AfterRecordManagerDelete != null) AfterRecordManagerDelete(Record);
                transaction.Commit();
            }
            return res;
        }

        internal virtual void PostRegistrationEvent()
        {
            if (RecordManagerPostRegistration != null)
                RecordManagerPostRegistration(null);
        }

        internal void DoTableVersionChanged(int version)
        {
            if (TableVersionChanged != null)
                TableVersionChanged(version);
        }
    }

    public class InVirtualTable : InTable
    {

        internal override bool IsVirtual
        {
            get { return true; }
        }

        internal override bool Read(object Record)
        {
            return false;
        }

        internal override int Delete(object Record)
        {
            return 0;
        }

        internal override int Insert(object Record)
        {
            return 0;
        }

        internal override int Update(object Record)
        {
            return 0;
        }


    }

    [InTable]
    internal class ObjectVersions
    {
        private string _name;

        [PrimaryKey]
        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }

        private int _version = -1;

        public int Version
        {
            get { return _version; }
            set { _version = value; }
        }
    }
    
    public class ManagerTransaction : IDisposable
    {
        RecordManager manager;
        bool complete = false;
        internal ManagerTransaction(RecordManager Manager)
        {
            manager = Manager;
            if (manager.TransactionLevel == 0)
            {
                RecordManager.ReopenConnection(manager.Connection);
                manager.transaction = manager.Connection.BeginTransaction();
            }
            manager.TransactionLevel++;
        }

        ~ManagerTransaction()
        {
            Dispose();
            //throw new ApplicationException("That has never to be happened! Check the transaction enclosing.");
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            if (!complete) 
                Rollback();
            manager.TransactionLevel--;
            if (manager.TransactionLevel == 0)
            {
                manager.transaction = null;
            };
        }

        public void Commit()
        {
            complete = true;
            if (manager.TransactionLevel==1)
                manager.transaction.Commit();
        }

        public void Rollback()
        {
            complete = true;
            if (manager.TransactionLevel==1) 
                manager.transaction.Rollback();
        }

    }

    public delegate void RecordOperation(RecordManager Manager, Object obj);

    public class RecordManager:IDisposable
    {
        public event EventHandler Disposed = null;

        public class PooledConnection : IDisposable
        {
            public event EventHandler Disposed = null;
            internal DbConnection conn = null;
            internal LinkedList<DbConnection> pool = null;
            internal PooledConnection(DbConnection Connection, LinkedList<DbConnection> Pool)
            {
                conn = Connection;
                pool = Pool;
            }

            public void Dispose()
            {
                if (pool!=null) 
                lock (pool)
                {
                    pool.AddLast(conn);
                    if (Disposed != null) Disposed(this, null);
                }
            }
        }
        public event RecordOperation OnRecordWritten=null;
        public event RecordOperation OnRecordDeleted=null;
        
        private CacheCollector cache = new CacheCollector();

        static Hashtable managers = new Hashtable();

        public static ICollection Managers
        {
            get
            {
                return managers.Values;
            }
        }
        public CacheCollector Cache
        {
            get { return cache; }
        }

        public void Dispose()
        {
            if (Disposed != null) Disposed(this, null);
            cache.Dispose();
        }

        internal static void ReopenConnection(DbCommand cmd)
        {
            ReopenConnection(cmd.Connection);
        }

        internal static void ReopenConnection(DbConnection conn)
        {
            if (conn.State == ConnectionState.Open)
                return;

            if (conn.State == ConnectionState.Closed)
                conn.Open();

            if (conn.State == ConnectionState.Broken)
            {
                conn.Close();
                conn.Open();
            }

        }


        /// <summary>
        /// Default record manager specific to each thread.
        /// </summary>
        public static RecordManager Default
        {
            get
            {
                RecordManager _default = (RecordManager)managers[Thread.CurrentThread];
                if (_default == null)
                    throw new ApplicationException("Default Record Manager is not defined");
                return _default;
            }
            set
            {
                if (value == null)
                    managers.Remove(Thread.CurrentThread);
                else
                    managers[Thread.CurrentThread] = value;
            }
        }
        /// <summary>
        /// Check the default Record manager for current thread is defined.
        /// </summary>
        public static bool DefaultIsDefined
        {
            get
            {
                return managers.ContainsKey(Thread.CurrentThread);
            }
        }


        IProviderSpecifics specifics;
        internal DbTransaction transaction = null;
        internal int TransactionLevel=0;

        LinkedList<DbConnection> ConnectionPool = new LinkedList<DbConnection>();
        LinkedList<PooledConnection> ConnectionStack = new LinkedList<PooledConnection>();

        /// <summary>
        /// Try to close all unused extra connections from connection pool.
        /// </summary>
        public void FlushConnectionPool()
        {
            lock (ConnectionPool)
            {
                while (ConnectionPool.First != null)
                {
                    ConnectionPool.First.Value.Close();
                    ConnectionPool.RemoveFirst();
                }
            }
        }


        /// <summary>
        /// Emit all new SQL commands within new connection until returned object will not be disposed.
        /// Disposing the returned object return to state before method call.
        /// </summary>
        /// <returns></returns>
        public PooledConnection WithinSeparateConnection()
        {
            lock (ConnectionPool)
            {
                if (ConnectionPool.First == null)
                {
                    DbConnection NewConn = (DbConnection)Activator.CreateInstance(specifics.Connection.GetType());
                    NewConn.ConnectionString = specifics.Connection.ConnectionString;
                    NewConn.Open();
                    ConnectionPool.AddLast(NewConn);
                    NewConn.Disposed += new EventHandler(
                        delegate (object sender, EventArgs e){
                            lock (ConnectionPool)
                            {
                                ConnectionPool.Remove(sender as DbConnection);
                            }
                        });
                }
               
                PooledConnection pc=new PooledConnection(ConnectionPool.First.Value,ConnectionPool);
                ConnectionPool.RemoveFirst();
                pc.Disposed += new EventHandler(
                        delegate(object sender, EventArgs e)
                        {
                            lock (ConnectionStack)
                            {
                                ConnectionStack.Remove(sender as PooledConnection);
                            }
                        });
                pc.conn.Disposed += new EventHandler(
                        delegate(object sender, EventArgs e)
                        {
                            lock (ConnectionStack)
                            {
                                ConnectionStack.Remove(pc);
                            }
                            pc.pool = null;
                        });

                lock (ConnectionStack)
                {
                    ConnectionStack.AddLast(pc);
                }

                return pc;
            }
        }


        public DbConnection Connection
        { 
            get {
                lock (ConnectionStack)
                {
                    return ConnectionStack.Last != null ? ConnectionStack.Last.Value.conn : specifics.Connection;
                }
            }
        }

        private Hashtable tables = new Hashtable();
        private Hashtable table_names = new Hashtable();

        internal void ClearRegistrations()
        {
            tables.Clear();
            table_names.Clear();
        }

        public RecordManager(IProviderSpecifics ProviderSpecifics, params Type[] RegisterTypes)
        {
            specifics = ProviderSpecifics;
            InitStructure(RegisterTypes);
        }

        public ManagerTransaction BeginTransaction()
        {
            return new ManagerTransaction(this);
        }

        public DbCommand CreateCommand(string Command)
        {
            return CreateCommand(Command, new object[] { });
        }

        public DbCommand CreateCommand(string command, params object[] parameters )
        {
            DbCommand cmd = Connection.CreateCommand();
            cmd.CommandText = command;
            int i = 0;
            while (i < parameters.Length)
            {
                string pname = parameters[i++].ToString();
                object pvalue = parameters[i++];
                DbParameter p = specifics.CreateParameter(pname, pvalue);
                cmd.Parameters.Add(p);
            }

            if (parameters.Length > 0)
            {
                ReopenConnection(cmd);
                cmd.Prepare();
            }
            
            return cmd;
        }

        public int RunCommand(string command, params object[] parameters)
        {
            using (DbCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteNonQuery();
        }

        public object RunScalarCommand(string command, params object[] parameters)
        {
            using (DbCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteScalar();
        }

        public DbDataReader CreateReader(string command, params object[] parameters)
        {
            using (DbCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteReader();
        }

        public IEnumerable RegisteredTypes
        {
            get { return tables.Values; }
        }

        public Type GetTypeFromTableName(string TableName)
        {
            return (Type)table_names[TableName];
        }

        internal void TryToRegisterAsActiveRecord(Type type)
        {
            if (type.IsAbstract) return;
            if (tables.ContainsKey(type))
                throw new Exception(string.Format("Object {0} already registered as ActiveRecord", type.ToString()));
            if (type.IsDefined(typeof(InTable), true))
            {
                InTable table = (InTable)((ICloneable)Attribute.GetCustomAttribute(type, typeof(InTable))).Clone();
                table.manager = this;
                

                if (table.Name == string.Empty) table.Name = type.Name;
                if (table_names.ContainsKey(table.Name))
                    throw new Exception(string.Format("Table name {0} already used bu ActiveRecord object {1}", table.Name, type.ToString()));

                List<InField> fields = new List<InField>();
                List<InField> primary_fields = new List<InField>();

                foreach (MethodInfo method in type.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public))
                {
                    foreach (Attribute mattr in Attribute.GetCustomAttributes(method))
                    {
                        if ((mattr.GetType() == typeof(BeforeRecordManagerDelete)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.BeforeRecordManagerDelete += new RecordManagerEvent(delegate(object o) { CallMethod(m, o); });
                        }

                        if ((mattr.GetType() == typeof(AfterRecordManagerDelete)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterRecordManagerDelete += new RecordManagerEvent(delegate(object o) { CallMethod(m, o); });
                        }

                        if ((mattr.GetType() == typeof(BeforeRecordManagerWrite)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.BeforeRecordManagerWrite += new RecordManagerWriteEvent(delegate(object o, ref bool handled)
                            {
                                object[] prm = new object[] { handled };
                                CallMethod(m, o,  prm);
                                handled = (bool)prm[0];
                            });
                        }

                        if ((mattr.GetType() == typeof(AfterRecordManagerRead)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterRecordManagerRead += new RecordManagerEvent(delegate(object o) { CallMethod(m, o); });
                        }

                        if ((mattr.GetType() == typeof(RecordSetInsert)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.RecordSetInsert += new RecordSetEvent(delegate(object o, object rs) { m.Invoke(o, new object[] { rs }); });
                        }

                        if ((mattr.GetType() == typeof(RecordSetRemove)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.RecordSetRemove += new RecordSetEvent(delegate(object o, object rs) { m.Invoke(o, new object[] { rs }); });
                        }

                        if ((mattr.GetType() == typeof(AfterRecordManagerWrite)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterRecordManagerWrite += new RecordManagerEvent(delegate(object o) { CallMethod(m, o); });
                        }

                        if ((mattr.GetType() == typeof(RecordManagerPostRegistration)) && (method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.RecordManagerPostRegistration += new RecordManagerEvent(delegate(object o) { CallMethod(m, null); });
                        }

                        if ((mattr.GetType() == typeof(OnTableVersionChange)) && (method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.TableVersionChanged += new RecordManagerEvent(delegate(object o) { CallMethod(m, null,o); });
                        }

                    }
                }

                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.IsDefined(typeof(ExcludeFromTable), true)) continue;
                    InField field = (InField)Attribute.GetCustomAttribute(type, typeof(InField));
                    if (field == null) field = new InField();
                    if (field.Name == string.Empty) field.Name = prop.Name;
                    field.IsPrimary = prop.IsDefined(typeof(PrimaryKey), true);
                    foreach (PrimaryKey pk in prop.GetCustomAttributes(typeof(PrimaryKey), true))
                    {
                        if (pk._GenerateSQL_PK == false) field.GenerateSQL_PK = false;
                    }
                    field.IsAutoincrement = prop.IsDefined(typeof(Autoincrement), true);
                    field.field_type = prop.PropertyType;
                    field.prop = prop;

                    field.IsIndexed = prop.IsDefined(typeof(Indexed), true);

                    if (prop.CanWrite) fields.Add(field);
                    if (field.IsPrimary)
                    {
                        if (primary_fields.Count > 0)
                            throw new Exception(string.Format("Can't define more than one field for primary index on object {0} ", type.ToString()));
                        primary_fields.Add(field);
                    }

                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                if ((table.with_replica) && (table.IsVirtual))
                {
                    throw new Exception(string.Format("Replica is not supported on virtual table {0}", table.Name));
                }

                table.fields = fields.ToArray();
                table.primary_fields = primary_fields.ToArray();
                tables[type] = table;
                table_names[table.Name] = type;
                table.basetype = type;
                if (!table.IsVirtual)
                {
                    
                    ObjectVersions ov = new ObjectVersions();
                    if (type.IsInstanceOfType(ov))
                    {
                        try
                        {
                            RunCommand(table.CreateTableStatement());
                        }
                        catch (Exception)
                        {
                        }
                        table.InitSqlMethods();
                    }
                    ov.Name = table.Name;
                    try
                    {
                    Read(ov);
                    }
                    catch (Exception) 
                    {
                    }

                    List<TableVersion> attrs = new List<TableVersion>((IEnumerable<TableVersion>)Attribute.GetCustomAttributes(type, typeof(TableVersion), true));
                    attrs.Insert(0, new TableVersion(0, TableAction.Recreate));
                    attrs.Sort();
                    foreach (TableVersion update in attrs)
                    {
                        if (update.Version > ov.Version)
                        {
                            try
                            {
                                if (update.Action == TableAction.Recreate)
                                {
                                    string s = update.SQLCode;
                                    update.SQLCode = table.DropTableStatement();
                                    RunCommand(update.SQLCode);
                                    update.SQLCode = table.CreateTableStatement();
                                    RunCommand(update.SQLCode);
                                    update.SQLCode = s;
                                }
                                if (
                                    (update.Action == TableAction.RunSQL)
                                    ||
                                    (update.Action == TableAction.Recreate)
                                    &&
                                    (update.SQLCode != string.Empty)
                                    )
                                RunCommand(update.SQLCode);

                            table.DoTableVersionChanged(update.Version);
                            }
                            catch (Exception E)
                            {
                                throw new Exception(
                                    string.Format("{0} when upgrading table {1} to version {2} with {3} action and command:\n{4}",
                                    E.Message,
                                    table.Name,
                                    update.Version,
                                    update.Action.ToString(),
                                    update.SQLCode), E);
                            }
                            ov.Version = update.Version;
                            Write(ov);
                        } 
                    }
                    table.InitSqlMethods();
                }
            }
        }

        private void CallMethod(MethodInfo m, object o,  params object[] prms)
        {
            ParameterInfo[] pi = m.GetParameters();
            object[] prm = new object[pi.Length];
            int pos = 0;
            Hashtable backref = new Hashtable();
            foreach (ParameterInfo p in pi)
            {
                prm[pos]=p.DefaultValue;
                if (p.ParameterType.IsInstanceOfType(this))
                    prm[pos] = this; else 
                {
                    int inpos = 0;
                    foreach (object so in prms) {
                        if (p.ParameterType.IsInstanceOfType(so))
                        {
                            prm[pos] =so;
                            break;
                        }

                    if ((p.ParameterType.IsByRef) && (p.ParameterType.FullName==so.GetType().FullName+"&"))
                    {
                        prm[pos] = so;
                        backref[pos]=inpos;
                        break;
                    }
                    inpos++;
                    }
                }
                pos++;
            }
                m.Invoke(o, prm);

                foreach (DictionaryEntry de in backref)
                    prms[(int)de.Value]=prm[(int)de.Key];
        }

        internal void InitStructure(params Type[] types)
        {

            TryToRegisterAsActiveRecord(typeof(ObjectVersions));

            using (ManagerTransaction transaction = BeginTransaction())
            {
                foreach (Type t in types)
                {
                    TryToRegisterAsActiveRecord(t);
                }
                transaction.Commit();
            }

            using (ManagerTransaction transaction = BeginTransaction())
            {
                foreach (InTable t in tables.Values)
                    t.PostRegistrationEvent();
                transaction.Commit();
            }

        }

        public bool Read(Object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.Read(Record);
        }


        public int Write(Object Record)
        {
            int res = 0;
            InTable table = ActiveRecordInfo(Record.GetType());
            using (ManagerTransaction t = BeginTransaction())
            {
                res = table.Write(Record);
                if (OnRecordWritten != null) OnRecordWritten(this, Record);
                t.Commit();
            }
            return res;
        }

        public int Delete(Object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            int res=0;
            using (ManagerTransaction t = BeginTransaction())
            {
            res=table.Delete(Record);
            if (OnRecordDeleted != null) OnRecordDeleted(this, Record);
            t.Commit();
            };
            return res;
        }


        internal object PKEY(object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.PKEYValue(Record);
        }

        internal InTable ActiveRecordInfo(Type type, bool withexception)
        {
            InTable table = (InTable)tables[type];
            if ((table == null) && (withexception)) throw new NotActiveRecordException(string.Format("Can't use {0} as Acive Record object", type.Name));
            return table;
        }

        internal InTable ActiveRecordInfo(Type type)
        {
            return ActiveRecordInfo(type, true);
        }
 
        public string SqlType(InField f) {
            return specifics.GetSqlType(f.FieldType);
        }

        public bool IsObjectExists(Type type, string statement, params object[] prm)
        {
            bool exist = false;
            InTable t=ActiveRecordInfo(type);
            using (DbDataReader rd = CreateReader("select 1 from " + AsFieldName(t.Name) + " where " + statement, prm))
            {
                exist=rd.Read();
                rd.Close();
            }
            return exist;
        }
        
        internal DbParameter CreateParameter(InField f)
        {
            return specifics.CreateParameter(f.Name, f.FieldType);
        }

        public DbParameter CreateParameter(string name, Type type)
        {
            return specifics.CreateParameter(name, type);
        }

        public string AsFieldName(string s)
        {
            return specifics.AsFieldName(s);
        }

        public string AsFieldParam(string s)
        {
            return specifics.AsFieldParam(s);
        }

        public string WhereExpression(string field,string operation)
        {
            return string.Format("{0}{1}{2}", AsFieldName(field), operation, AsFieldParam(field));
        }

        public string WhereEqual(string field)
        {
            return WhereExpression(field, "=");
        }


        internal string AdoptSelectCommand(string select, InField[] fields)
        {
            return specifics.AdoptSelectCommand(select, fields);
        }

        internal string Autoincrement(string Name)
        {
            return specifics.AutoincrementStatement(Name);
        }
    }
}
