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
            if (pkeycolumns == String.Empty) throw new Exception("Can't update record until primary key defined");

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


        internal void InitContent()
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



        internal bool Read(object Record)
        {
            bool res = false;
            int i=0;
            foreach (InField field in primary_fields)
            {
                FillCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
            }
            i = 0;
            using (SQLiteDataReader reader = FillCmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    foreach (InField field in fields)
                    {
                        field.prop.SetValue(Record,reader.GetValue(i++),null);
                    }
                    res = true;
                }
                reader.Close();
            }
            return res;
        }

 



        internal int Update(object Record)
        {
            int i = 0;
            foreach (InField field in fields)
            {
                UpdateCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
            }
            return UpdateCmd.ExecuteNonQuery();
        }

        internal int Insert(object Record)
        {
            int i = 0;
            foreach (InField field in fields)
            {
                InsertCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
            }
            return InsertCmd.ExecuteNonQuery();
        }

        internal int Write(object Record)
        {
            int r=Update(Record);
            if (r == 0) r = Insert(Record);
            return r;
        }

        internal int Delete(object Record)
        {
            int i = 0;
            foreach (InField field in primary_fields)
            {
                DeleteCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
            }
            return DeleteCmd.ExecuteNonQuery();
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

                    fields.Add(field);
                    if (field.IsPrimary) {
                        if (primary_fields.Count>0) 
                            throw new Exception(string.Format("Can't define more than one field for primary ondex on object {0} ",type.ToString()));
                        primary_fields.Add(field);
                    }

                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                if ((table.with_replica) && ((primary_fields.Count!=1) || (primary_fields[0].Name.ToLower()!="id")))
                throw new Exception(string.Format("Define a property with ID name as primary index on {0} to be ready for replication",type.ToString()));


                table.fields = fields.ToArray();
                table.primary_fields = primary_fields.ToArray();
                tables[type] = table;
                table_names[table.Name] = type;
                table.basetype = type;
                table.InitContent();

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

        public static void Read(Object Record)
        {
            InTable table = (InTable) tables[Record.GetType()];
            if (table == null) throw new Exception(string.Format("Can't read object {0} as Active Record",Record.ToString()));
            table.Read(Record);
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

    public class RecordSet<T>:IEnumerable
    {

        private Hashtable index = new Hashtable();
        private List<T> list = new List<T>();
        private InTable table = null;

        protected void InitUnderlyingClass(Type RecordType)
        {
            table = RecordBase.ActiveRecordInfo(RecordType);
        }

        

        public RecordSet()
        {
            if (this.GetType()==typeof(RecordSet<T>))
            InitUnderlyingClass(typeof(T));
        }

        public T Add(T record)
        {
            index.Add(table.PKEYValue(record), record);
            list.Add(record);
            return record;
        }

        public void Remove(T record)
        {
            index.Remove(table.PKEYValue(record));
            list.Remove(record);
        }

        public T this[object key]
        {
            get { return (T)index[key];}
        }

        public T this[int pos]
        {
            get { return (T)list[pos]; }
        }

        public IEnumerator GetEnumerator()
        {
            return list.GetEnumerator();
        }

        public void Fill(string filter, string orderby, params object[] filter_params)
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
                        if (ci==null) 
                            throw new Exception(string.Format("Can't create instance of {0} without default constructor",table.basetype.Name));
                        T instance = (T)ci.Invoke(null);
                        foreach (InField field in table.fields)
                        {
                            try
                            {
                                field.prop.SetValue(instance, reader.GetValue(i++), null);

                            }
                            catch
                            {
                                field.prop.SetValue(
                                    instance,
                                    field.prop.PropertyType.GetConstructor(System.Type.EmptyTypes).Invoke(null),
                                    null);
                            }
                        }
                        Add(instance);
                    }
                    reader.Close();
                }
                transaction.Commit();
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
            list.Sort();
        }

        public void Sort(Comparison<T> comparision)
        {
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
