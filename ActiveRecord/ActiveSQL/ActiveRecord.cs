using System;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;



namespace Softlynx.ActiveSQL
{
    public interface IProviderSpecifics
    {
        DbParameter CreateParameter(InField f);
        string GetSqlType(InField f);
        DbType GetDbType(InField f);
        string AsFieldName(string s);
        string AsFieldParam(string s);
        DbConnection Connection
        {
            get;
        }
        void ExtendConnectionString(string key, string value);
    }

    #region Common attributes
    public enum TableAction { None, RunSQL, Recreate };
    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ExcludeFromTable : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class Indexed : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class WithReplica : Attribute
    {
    }

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
    public class BeforeRecordManagerDelete : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerDelete : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class BeforeRecordManagerWrite : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerWrite : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterRecordManagerRead : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = true)]
    public class AfterDatabaseOpened : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class PrimaryKey : NamedAttribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetAssign:NamedAttribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetInsert : NamedAttribute
    {
    }

    public class RecordSetRemove : NamedAttribute
    {
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
    public class TableVersion : NamedAttribute
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

    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class InField : NamedAttribute
    {
        internal bool IsPrimary = false;
        internal bool IsIndexed = false;
        internal Type field_type = typeof(object);
        internal PropertyInfo prop = null;
        //internal UntypedRecordSet foreign_key = null;

        internal String CreateColumnStatement(RecordManager manager)
        {
            return String.Format("{0} {1}", manager.AsFieldName(Name), manager.SqlType(this));
        }

        public Type FieldType
        {
            get { return field_type; }
        }
    }


    #endregion
    
    delegate void RecordManagerEvent(object o);
    delegate void RecordSetEvent(object o, object recordset);

    public interface IActiveRecordWriter
    {
        bool ActiveRecordWrite(RecordManager manager);
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class InTable : NamedAttribute,ICloneable

    {
        internal RecordManager manager = null;

        private DbCommand InsertCmd = null;
        private DbCommand UpdateCmd = null;
        private DbCommand DeleteCmd = null;
        private DbCommand FillCmd = null;
        internal bool with_replica = false;

        internal InField[] fields;
        internal InField[] primary_fields;
        internal Type basetype = default(Type);
        internal Hashtable foreign_keys = new Hashtable();
        internal event RecordManagerEvent BeforeRecordManagerDelete = null;
        internal event RecordManagerEvent AfterRecordManagerDelete = null;
        internal event RecordManagerEvent BeforeRecordManagerWrite = null;
        internal event RecordManagerEvent AfterRecordManagerWrite = null;
        internal event RecordManagerEvent AfterRecordManagerRead = null;
        internal event RecordManagerEvent AfterDatabaseOpened = null;
        internal event RecordSetEvent RecordSetInsert = null;
        internal event RecordSetEvent RecordSetRemove = null;

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
            if (primary_fields.Length > 0)
            {
                string flags = string.Empty;
                s += String.Format(",\nPRIMARY KEY ({0}) {1}",
                    ColumnsList(primary_fields),
                    flags
                    );
            }
            else
            {
                throw new Exception(String.Format("Primary key not defined for table {0}", Name));
            }
            s += String.Format("\n);\n");
            foreach (InField f in fields)
            {
                if (f.IsIndexed) s += string.Format("CREATE INDEX {0}_idx on {1}({0});\n", manager.AsFieldName(f.Name), manager.AsFieldName(Name));
            }
            return s;
        }

        string InsertCommandText()
        {
            return String.Format("INSERT INTO {0}({1}) values ({2})",
                manager.AsFieldName(Name),
                ColumnsList(fields),
                ColumnsList(fields, true)
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

            return String.Format("SELECT {0} FROM {1} where ({2})",
                ColumnsList(fields),
                manager.AsFieldName(Name),
                pkeycolumns
                );
        }


        internal virtual void InitContent()
        {
            InsertCmd = manager.CreateCommand(InsertCommandText());
            UpdateCmd = manager.CreateCommand(UpdateCommandText());
            DeleteCmd = manager.CreateCommand(DeleteCommandText());
            FillCmd = manager.CreateCommand(FillCommandText());

           
            foreach (InField field in fields)
            {

                DbParameter prm = manager.CreateParameter(field);
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
            if (res && (AfterRecordManagerRead != null))
                AfterRecordManagerRead(Record);
            return res;
        }


        internal virtual int Update(object Record)
        {
            foreach (InField field in fields)
            {
                UpdateCmd.Parameters[field.Name].Value = field.prop.GetValue(Record, null);
            }
            return UpdateCmd.ExecuteNonQuery();
        }

        internal virtual int Insert(object Record)
        {
            foreach (InField field in fields)
            {
                InsertCmd.Parameters[field.Name].Value = field.prop.GetValue(Record, null);
            }
            return InsertCmd.ExecuteNonQuery();
        }

        internal virtual int Write(object Record)
        {
            using (ManagerTransaction t=manager.BeginTransaction())
            {
                if (BeforeRecordManagerWrite != null) BeforeRecordManagerWrite(Record);
                bool write_handled = false;
                int r = 0;
                if (Record is IActiveRecordWriter)
                {
                    write_handled = ((IActiveRecordWriter)(Record)).ActiveRecordWrite(manager);
                }
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
            int res = 0;
            using (ManagerTransaction transaction = manager.BeginTransaction())
            {

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

        internal virtual void CallAfterDatabaseOpened()
        {
            if (AfterDatabaseOpened != null)
                AfterDatabaseOpened(null);
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
        internal ManagerTransaction(RecordManager Manager)
        {
            manager = Manager;
            if (manager.TransactionLevel == 0)
            {
                manager.transaction = manager.Connection.BeginTransaction();
            }
            manager.TransactionLevel++;
        }

        public void Dispose()
        {
            manager.TransactionLevel--;
            if (manager.TransactionLevel == 0)
            {
                manager.transaction = null;
            };
        }

        public void Commit()
        {
            if (manager.TransactionLevel==1)
                manager.transaction.Commit();
        }

        public void Rollback()
        {
            if (manager.TransactionLevel==1) 
                manager.transaction.Rollback();
        }

    }


    public class RecordManager
    {
        static RecordManager _default = null;

        public static RecordManager Default
        {
            get
            {
                if (_default == null)
                    throw new ApplicationException("Default Record Manager is not defined");
                return _default;
            }
            set
            {
                _default = value;
            }
        }

        IProviderSpecifics specifics;
        internal DbTransaction transaction = null;
        internal int TransactionLevel=0;

        public DbConnection Connection
        { 
            get {return specifics.Connection;}
        }

        private static Hashtable tables = new Hashtable();
        private static Hashtable table_names = new Hashtable();

        internal static void ClearRegistrations()
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

        public DbCommand CreateCommand(string command, params object[] parameters)
        {
            DbCommand cmd = Connection.CreateCommand();
            cmd.CommandText = command;
            int i = 0;
            while (i < parameters.Length)
            {
                string pname = parameters[i++].ToString();
                object pvalue = parameters[i++];
                cmd.Parameters.Add(new SqlParameter(pname, pvalue));
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

        internal void TryToRegisterAsActiveRecord(Type type)
        {
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
                            table.BeforeRecordManagerDelete += new RecordManagerEvent(delegate(object o) { m.Invoke(o, null); });
                        }

                        if ((mattr.GetType() == typeof(AfterRecordManagerDelete)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterRecordManagerDelete += new RecordManagerEvent(delegate(object o) { m.Invoke(o, null); });
                        }

                        if ((mattr.GetType() == typeof(BeforeRecordManagerWrite)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.BeforeRecordManagerWrite += new RecordManagerEvent(delegate(object o) { m.Invoke(o, null); });
                        }

                        if ((mattr.GetType() == typeof(AfterRecordManagerRead)) && (!method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterRecordManagerRead += new RecordManagerEvent(delegate(object o) { m.Invoke(o, null); });
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
                            table.AfterRecordManagerWrite += new RecordManagerEvent(delegate(object o) { m.Invoke(o, null); });
                        }

                        if ((mattr.GetType() == typeof(AfterDatabaseOpened)) && (method.IsStatic))
                        {
                            MethodInfo m = method;
                            table.AfterDatabaseOpened += new RecordManagerEvent(delegate(object o) { m.Invoke(null, null); });
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
                    field.field_type = prop.PropertyType;
                    field.prop = prop;


                    field.IsIndexed = prop.IsDefined(typeof(Indexed), true);

                    if (prop.CanWrite) fields.Add(field);
                    if (field.IsPrimary)
                    {
                        if (primary_fields.Count > 0)
                            throw new Exception(string.Format("Can't define more than one field for primary ondex on object {0} ", type.ToString()));
                        primary_fields.Add(field);
                    }

                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                if ((table.with_replica) && (table.IsVirtual))
                {
                    throw new Exception(string.Format("Replica is not supported on virtual table {0}", table.Name));
                }

                if ((table.with_replica) && ((primary_fields.Count != 1) || (primary_fields[0].Name.ToLower() != "id")))
                    throw new Exception(string.Format("Define a property with ID name as primary index on {0} to be ready for replication", type.ToString()));


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
                        catch (DbException)
                        {
                        }
                        table.InitContent();
                    }
                    ov.Name = table.Name;
                    try
                    {
                    Read(ov);
                    }
                    catch (DbException) 
                    {
                    }

                    List<Attribute> attrs = new List<Attribute>(Attribute.GetCustomAttributes(type, typeof(TableVersion), true));
                    attrs.Insert(0, new TableVersion(0, TableAction.Recreate));
                    foreach (TableVersion update in attrs)
                    {
                        if (update.Version > ov.Version)
                        {
                            try
                            {
                                if (update.Action == TableAction.Recreate)
                                {
//                                    if (table.with_replica)
//                                        Session.replica.DropTableReplicaLogSchema(table.Name);
                                    RunCommand(table.DropTableStatement());
                                    RunCommand(table.CreateTableStatement());
                                    table.InitContent();
//                                    if (table.with_replica) Session.replica.CreateTableReplicaLogSchema(table.Name);
                                }
                                if (
                                    (update.Action == TableAction.RunSQL)
                                    ||
                                    (update.Action == TableAction.Recreate)
                                    &&
                                    (update.SQLCode != string.Empty)
                                    )
                                RunCommand(update.SQLCode);
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
                        } else
                            table.InitContent();
                    }

                }
            }
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
                foreach (InTable t in tables.Values)
                    t.CallAfterDatabaseOpened();
                transaction.Commit();
            }
        }

        public bool Read(Object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.Read(Record);
        }


        public void Write(Object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            table.Write(Record);
        }

        public int Delete(Object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.Delete(Record);
        }


        internal object PKEY(object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.PKEYValue(Record);
        }

        internal InTable ActiveRecordInfo(Type type)
        {
            InTable table = (InTable)tables[type];
            if (table == null) throw new Exception(string.Format("Can't use {0} as Acive Record object", type.Name));
            return table;
        }

 
        public string SqlType(InField f) {
            return specifics.GetSqlType(f);
        }

        public DbParameter CreateParameter(InField f)
        {
            return specifics.CreateParameter(f);
        }
        internal string AsFieldName(string s)
        {
            return specifics.AsFieldName(s);
        }

        internal string AsFieldParam(string s)
        {
            return specifics.AsFieldParam(s);
        }


    }
}
