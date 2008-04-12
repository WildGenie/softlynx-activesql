using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Reflection;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;

namespace Softlynx.SQLiteDataset.ActiveRecord
{

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ExcludeFromTable : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ForeignKey : Attribute
    {
        private Type _fClass;

        public Type ForeignClass
        {
            get { return _fClass; }
            set { _fClass = value; }
        }

        public ForeignKey(Type ForeignKeyClass)
        {
            ForeignClass = ForeignKeyClass;
        }

    }

    [AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
    public class WithReplica : Attribute
    {
    }

    public class NamedAttribute:Attribute
    {
        private string _name = string.Empty;
        public string Name
        {
            get { return _name; }
            set { _name = value; }
        }
    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class PrimaryKey : NamedAttribute
    {
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = true)]
    public class TableVersion : NamedAttribute
    {
        private int _version;

        public int Version
        {
            get { return _version; }
            set { _version = value; }
        }

        private string _sql_code;

        public string SQLCode
        {
            get { return _sql_code; }
            set { _sql_code = value; }
        }

        public TableVersion(int version, string sql_code)
        {
            Version = version;
            SQLCode = sql_code;
        }

    }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class InField : NamedAttribute
    {
        internal bool IsPrimary = false;
        internal Type field_type = typeof(object);
        internal PropertyInfo prop = null;
        internal UntypedRecordSet foreign_key = null;
        
        internal String CreateColumnStatement()
        {
            string flags = string.Empty;
            return String.Format("{0} {1}{2}", Name,field_type.Name,flags);
        }
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = true, AllowMultiple = false)]
    public class InTable : NamedAttribute
    {
        private SQLiteCommand InsertCmd = null;
        private SQLiteCommand UpdateCmd = null;
        private SQLiteCommand DeleteCmd = null;
        private SQLiteCommand FillCmd = null;
        internal bool with_replica = false;

        internal InField[] fields;
        internal InField[] primary_fields;
        internal Type basetype = default(Type);
        internal Hashtable foreign_keys = new Hashtable();

        internal object PKEYValue(object o)
        {
            return primary_fields[0].prop.GetValue(o, null);
        }

        internal virtual bool IsVirtual
        {
            get { return false; }
        }

        internal void SetPKEYValue(object o,object v)
        {
            primary_fields[0].prop.SetValue(o,v,null);
        }

        internal String ColumnsList(ICollection fromcolums, string columnprefix, string separator)
        {
            int i = 0;
            String[] clist = new string[fromcolums.Count];
            foreach (Object c in fromcolums)
            {
                string cname = String.Empty;
                if (c is InField) cname = (c as InField).Name;
                if (cname == String.Empty) throw new Exception("Can't get column name from type " + fromcolums.GetType().ToString());
                clist[i++] = string.Format("{0}{1}", columnprefix, cname);
            };
            return String.Join(separator, clist);
        }

        internal String ColumnsList(ICollection fromcolums, string columnprefix)
        {
            return ColumnsList(fromcolums, columnprefix, ",");
        }

        internal String ColumnsList(ICollection fromcolums)
        {
            return ColumnsList(fromcolums, string.Empty, ",");
        }

        String CreateTableStatement()
        {
            String s = String.Format("CREATE TABLE IF NOT EXISTS {0} (\n", Name);
            String cols = String.Empty;
            foreach (InField col in fields)
            {
                if (cols != String.Empty) cols += ",\n";
                cols += String.Format("{0}", col.CreateColumnStatement());
            }
            s += cols;
            if (primary_fields.Length > 0)
            {
                string flags = string.Empty;
                //flags="ON CONFLICT REPLACE";
                s += String.Format(",\nPRIMARY KEY ({0}) {1}",
                    ColumnsList(primary_fields),
                    flags
                    );
            }
            else
            {
                throw new Exception (String.Format("Primary key not defined for table {0}",Name));
            }
            s += String.Format("\n);\n");
            return s;
        }

        string InsertCommandText()
        {
            return String.Format("INSERT INTO {0}({1}) values ({2})",
                Name,
                ColumnsList(fields),
                ColumnsList(fields, "@")
                );
        }

        string UpdateCommandText()
        {
            string pkeycolumns = string.Empty;
            string keyvalpairs = string.Empty;
            foreach (InField dc in fields)
            {
                if (keyvalpairs != String.Empty) keyvalpairs += ",";
                keyvalpairs += String.Format("{0}=@{0}", dc.Name);
            };

            foreach (InField dc in primary_fields)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}=@{0}", dc.Name);
            };
            if (pkeycolumns == String.Empty) throw new Exception(string.Format("Can't construct update quiery for type {0} until primary key is defined",Name));

            return String.Format("UPDATE {0} set {1} where ({2})",
                Name,
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
                pkeycolumns = String.Format("{0}=@{0}", dc.Name);
            };
            if (pkeycolumns == String.Empty) throw new Exception("Can't delete until primary key defined");
            return String.Format("DELETE FROM {0} where ({1})",
                Name,
                pkeycolumns
                );
        }

        string FillCommandText()
        {
            string pkeycolumns = string.Empty;
            foreach (InField dc in primary_fields)
            {
                if (pkeycolumns != String.Empty) pkeycolumns += ",";
                pkeycolumns = String.Format("{0}=@{0}", dc.Name);
            };

            return String.Format("SELECT {0} FROM {1} where ({2})",
                ColumnsList(fields),
                Name,
                pkeycolumns
                );
        }


        internal virtual void InitContent()
        {
            InsertCmd = Session.CreateCommand(InsertCommandText());
            UpdateCmd = Session.CreateCommand(UpdateCommandText());
            DeleteCmd = Session.CreateCommand(DeleteCommandText());
            FillCmd = Session.CreateCommand(FillCommandText());

            foreach (InField field in fields)
            {
                SQLiteParameter prm = new SQLiteParameter("@" + field.Name);
                InsertCmd.Parameters.Add(prm);
                UpdateCmd.Parameters.Add(prm);
            }
            
            foreach (InField field in primary_fields)
            {
                SQLiteParameter prm = new SQLiteParameter("@" + field.Name);
                DeleteCmd.Parameters.Add(prm);
                FillCmd.Parameters.Add(prm);
            }

            InsertCmd.Prepare();
            UpdateCmd.Prepare();
            DeleteCmd.Prepare();
            FillCmd.Prepare();
            Session.RunCommand(CreateTableStatement());
        }



        internal virtual bool Read(object Record)
        {
            bool res = false;
            foreach (InField field in primary_fields)
            {
                FillCmd.Parameters["@" + field.Name].Value = field.prop.GetValue(Record, null);
            }
            using (SQLiteDataReader reader = FillCmd.ExecuteReader())
            {
                int i = 0;
                if (reader.Read())
                {
                    foreach (InField field in fields)
                    {
                        Object v = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        field.prop.SetValue(Record,v,null);
                        i++;
                    }
                    res = true;
                }
                reader.Close();
            }
            return res;
        }


        internal virtual int Update(object Record)
        {
            foreach (InField field in fields)
            {
                UpdateCmd.Parameters["@"+field.Name].Value = field.prop.GetValue(Record, null);
            }
            return UpdateCmd.ExecuteNonQuery();
        }

        internal virtual int Insert(object Record)
        {
            foreach (InField field in fields)
            {
                InsertCmd.Parameters["@" + field.Name].Value = field.prop.GetValue(Record, null);
            }
            return InsertCmd.ExecuteNonQuery();
        }

        internal virtual int Write(object Record)
        {
            int r=Update(Record);
            if (r == 0) r = Insert(Record);
            return r;
        }

        internal virtual int Delete(object Record)
        {
            int i = 0;
            foreach (InField field in primary_fields)
            {
                DeleteCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
            }
            return DeleteCmd.ExecuteNonQuery();
        }
    }

    public class InVirtualTable : InTable
    {
        internal override void InitContent()
        {
        }
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

        private int _version;

        public int Version
        {
            get { return _version; }
            set { _version = value; }
        }
    }

    public static class RecordBase
    {
        private static Hashtable tables = new Hashtable();
        private static Hashtable table_names = new Hashtable();

        internal static void ClearRegistrations()
        {
            tables.Clear();
            table_names.Clear();
        }

        internal static void TryToRegisterAsActiveRecord(Type type)
        {
            if (tables.ContainsKey(type)) 
                throw new Exception(string.Format("Object {0} already registered as ActiveRecord",type.ToString()));
            if (type.IsDefined(typeof(InTable), true))
            {
                InTable table = (InTable)Attribute.GetCustomAttribute(type, typeof(InTable));
                if (table.Name == string.Empty) table.Name = type.Name;
                if (table_names.ContainsKey(table.Name))
                    throw new Exception(string.Format("Table name {0} already used bu ActiveRecord object {1}", table.Name,type.ToString()));

                List<InField> fields = new List<InField>();
                List<InField> primary_fields = new List<InField>();
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.IsDefined(typeof(ExcludeFromTable), true)) continue;
                    InField field = (InField)Attribute.GetCustomAttribute(type, typeof(InField));
                    if (field == null) field = new InField();
                    if (field.Name == string.Empty) field.Name = prop.Name;
                    field.IsPrimary = prop.IsDefined(typeof(PrimaryKey), true);
                    field.field_type = prop.PropertyType;
                    field.prop = prop;

                    if (prop.IsDefined(typeof(ForeignKey), true))
                    {
                        ForeignKey fk = (ForeignKey)Attribute.GetCustomAttribute(prop, typeof(ForeignKey));
                        field.foreign_key = new UntypedRecordSet(fk.ForeignClass);
                        table.foreign_keys[prop.Name] = field.foreign_key;
                    }

                    if (prop.CanWrite) fields.Add(field);
                    if (field.IsPrimary) {
                        if (primary_fields.Count>0) 
                            throw new Exception(string.Format("Can't define more than one field for primary ondex on object {0} ",type.ToString()));
                        primary_fields.Add(field);
                    }

                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                if ((table.with_replica) && (table.IsVirtual))
                {
                throw new Exception(string.Format("Replica is not supported on virtual table {0}", table.Name));
                }

                if ((table.with_replica) && ((primary_fields.Count!=1) || (primary_fields[0].Name.ToLower()!="id")))
                throw new Exception(string.Format("Define a property with ID name as primary index on {0} to be ready for replication",type.ToString()));


                table.fields = fields.ToArray();
                table.primary_fields = primary_fields.ToArray();
                tables[type] = table;
                table_names[table.Name] = type;
                table.basetype = type;
                table.InitContent();

                if (!table.IsVirtual)
                {
                    ObjectVersions ov = new ObjectVersions();
                    ov.Name = table.Name;
                    RecordBase.Read(ov);


                    foreach (TableVersion update in Attribute.GetCustomAttributes(type, typeof(TableVersion), true))
                    {
                        if (update.Version > ov.Version)
                        {
                            try
                            {
                                Session.RunCommand(update.SQLCode);
                            }
                            catch (Exception E)
                            {
                                throw new Exception(
                                    string.Format("{0} when upgrading table {1} to version {2} with command:\n{3}",
                                    E.Message,
                                    table.Name,
                                    update.Version,
                                    update.SQLCode), E);
                            }
                            ov.Version = update.Version;
                            RecordBase.Write(ov);
                        }
                    }

                    if (table.with_replica)
                    {
                        Session.replica.CreateTableReplicaLogSchema(table.Name);
                    }
                }
                }
        }

        internal static void InitStructure(params Type[] types)
        {

            using (SQLiteTransaction transaction = Session.Connection.BeginTransaction())
            {

                TryToRegisterAsActiveRecord(typeof(ObjectVersions));
                foreach (Type t in types)
                {
                    TryToRegisterAsActiveRecord(t);
                }
                transaction.Commit();
            }
        }

        public static bool Read(Object Record)
        {
            InTable table = (InTable) tables[Record.GetType()];
            if (table == null) throw new Exception(string.Format("Can't read object {0} as Active Record",Record.ToString()));
            return table.Read(Record);
        }


        public static void Write(Object Record)
        {
            InTable table = (InTable)tables[Record.GetType()];
            if (table == null) throw new Exception(string.Format("Can't write object {0} as Active Record", Record.ToString()));
            table.Write(Record);
        }

        public static int Delete(Object Record)
        {
            InTable table = (InTable)tables[Record.GetType()];
            if (table == null) throw new Exception(string.Format("Can't delete object {0} as Active Record", Record.ToString()));
            return table.Delete(Record);
        }


        public static object PKEY(object Record)
        {
            InTable table = (InTable)tables[Record.GetType()];
            if (table == null) throw new Exception(string.Format("Can't set PKEY for {0}", Record.ToString()));
            return table.PKEYValue(Record);
        }

        internal static InTable ActiveRecordInfo(Type type)
        {
            InTable table = (InTable)tables[type];
            if (table == null) throw new Exception(string.Format("Can't use {0} as Acive Record object", type.Name));
            return table;
        }

    }

    public class RecordSet<T>:IEnumerable,ICollection,IList
    {

        private Hashtable index = new Hashtable();
        private List<T> list = new List<T>();
        private InTable table = null;

        public void Clear()
        {
            lock (this)
            {
                index.Clear();
                list.Clear();
            }
        }

        public int Count { get { lock (this) return list.Count; } }

        public void CopyTo(Array array, int index)
        {
            lock (this)
            list.CopyTo((T[])array, index);
        }

        public Object SyncRoot { get { return this; } }

        public bool IsSynchronized { get {return true;}}

        protected void InitUnderlyingClass(Type RecordType)
        {
            table = RecordBase.ActiveRecordInfo(RecordType);
        }

        
        public delegate void WhenAddNew(T record);
        public delegate void WhenRemoveExisting(T record);

        public event WhenAddNew OnAdded;
        public event WhenRemoveExisting OnRemoved;

        public RecordSet()
        {
            if (this.GetType()==typeof(RecordSet<T>))
            InitUnderlyingClass(typeof(T));
        }

        public int Add(Object o)
        {
            lock (this)
            {
                Add((T)o);
                return list.Count-1;
            }
        }

        public bool Contains (Object record)
        {
            lock (this)
            {
                return index[table.PKEYValue(record)] != null;
            }
        }



        public T Add(T record)
        {
            if (record != null) lock (this)
                {
                    object pk = table.PKEYValue(record);
                    object po = index[pk];
                    if (po != null) list.Remove((T)po);
                    index[pk]=record;
                    list.Add(record);
                    if (OnAdded != null) OnAdded(record);
                }
            return record;
        }

        public bool Remove(T record)
        {
            if (record!=null) lock (this)
            {
                object idxv=table.PKEYValue(record);
                record = (T)index[idxv];
                if (record!=null)
                {
                    index.Remove(idxv);
                    if (list.Remove(record)) 
                    if (OnRemoved!= null) OnRemoved(record);
                    return true;
                }
            }
        return false;
        }

        public void Remove(Object record)
        {
            Remove((T)record);
        }

        public void RemoveAt(int pos)
        {
            Remove(this[pos]);
        }

        public object this[int pos]
        {
            get { lock (this) return list[pos]; }
            set { throw new Exception("Not supported"); }
        }

        public void Insert (int index,	Object value)
        {
            throw new Exception("Not supported");
        }

        public T this[object key]
        {
            get
            {
                lock (this)
                {
                    object o = index[key];
                    if ((o == null) && (!table.IsVirtual))
                    {
                        o = table.basetype.GetConstructor(System.Type.EmptyTypes).Invoke(null);
                        table.SetPKEYValue(o, key);
                        if (RecordBase.Read(o)) Add((T)o); else o = null;
                    }
                    return (T)o;
                }
            }
        }
        public bool IsReadOnly { get {return false;} }
        
        public bool IsFixedSize { get { return false; } }

        public int IndexOf(T record)
        {
            lock (this)
            {
                return list.IndexOf(record);
            }
        }

        public int IndexOf(Object record)
        {
            return IndexOf((T)record);
        }

        public IEnumerator GetEnumerator()
        {
            lock (this)
            return list.GetEnumerator();
        }

        public void Fill(string filter, string orderby, params object[] filter_params)
        {
            if (table.IsVirtual)
            {
                throw new Exception("Can't fill virual tables from database");
            }

            lock (this)
            {
                string cmd = String.Format("SELECT {0} from {1}", table.ColumnsList(table.fields), table.Name);
                if (filter != string.Empty)
                {
                    cmd += String.Format(" WHERE ({0})", filter);
                };

                if (orderby != string.Empty)
                {
                    cmd += String.Format(" ORDER BY ({0})", orderby);
                };



                using (DbTransaction transaction = Session.Connection.BeginTransaction(IsolationLevel.ReadUncommitted))
                {
                    using (DbDataReader reader = Session.CreateReader(cmd, filter_params))
                    {
                        list.Clear();
                        index.Clear();
                        while (reader.Read())
                        {
                            int i = 0;
                            ConstructorInfo ci = table.basetype.GetConstructor(System.Type.EmptyTypes);
                            if (ci == null)
                                throw new Exception(string.Format("Can't create instance of {0} without default constructor", table.basetype.Name));
                            T instance = (T)ci.Invoke(null);
                            foreach (InField field in table.fields)
                            {
                                Object v = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                i++;
                                try
                                {
                                    field.prop.SetValue(instance, v, null);
                                }
                                catch
                                {
                                }
                            }
                            Add(instance);
                        }
                        reader.Close();
                    }
                    transaction.Commit();
                }
            }
        }

        public void Fill(string filter, params object[] filter_params)
        {
            Fill(filter, string.Empty, filter_params);
        }

        public void Fill()
        {
            Fill(string.Empty, string.Empty);
        }

        public void Sort()
        {
            lock (this)
            list.Sort();
        }

        public void Sort(Comparison<T> comparision)
        {
            lock (this)
            list.Sort(comparision);
        }

        public UntypedRecordSet ForeignKey(string ForeignKeyName)
        {
            return (UntypedRecordSet)table.foreign_keys[ForeignKeyName];
        }
    }
    
    public class UntypedRecordSet : RecordSet<object> {
        public UntypedRecordSet(Type RecordType)
        {
            InitUnderlyingClass(RecordType);
        }
    };
}
