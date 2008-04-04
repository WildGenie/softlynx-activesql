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
        
        String ColumnsList(ICollection fromcolums, string columnprefix, string separator)
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

        String ColumnsList(ICollection fromcolums, string columnprefix)
        {
            return ColumnsList(fromcolums, columnprefix, ",");
        }

        String ColumnsList(ICollection fromcolums)
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

        internal Array Read(Type type, string filter, string orderby, params object[] filter_params)
        {
            ArrayList  res = new ArrayList();

            string cmd = String.Format("SELECT {0} from {1}", ColumnsList(fields),Name);
            if (filter != string.Empty)
            {
                cmd += String.Format(" WHERE ({0})", filter);
            };

            if (orderby != string.Empty)
            {
                cmd += String.Format(" ORDER BY ({0})", orderby);
            };

            

            using (DbTransaction transaction = FillCmd.Connection.BeginTransaction(IsolationLevel.ReadUncommitted))
            {
                using (DbDataReader reader = Session.CreateReader(cmd, filter_params))
                {
                    while (reader.Read())
                    {
                        int i = 0;
                        object instance = type.GetConstructor(System.Type.EmptyTypes).Invoke(null);
                        foreach (InField field in fields)
                        {
                            field.prop.SetValue(instance,reader.GetValue(i++),null);
                        }
                        res.Add(instance);
                    }

                    reader.Close();
                }
                transaction.Commit();
            }

            return res.ToArray(type);
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
                    fields.Add(field);
                    if (field.IsPrimary) primary_fields.Add(field);
                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                table.fields = fields.ToArray();
                table.primary_fields = primary_fields.ToArray();
                tables[type] = table;
                table_names[table.Name] = type;
                
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

        public static Array Read(Type type, string filter, string orderby, params object[] filter_params)
        {
            InTable table = (InTable)tables[type];
            if (table == null) throw new Exception(string.Format("Can't read object {0} as Active Record", type.FullName));
            return table.Read(type, filter, orderby, filter_params);

        }

        public static Array Read(Type type, string filter, params object[] filter_params)
        {
            return Read(type, filter, string.Empty, filter_params);
        }

        public static Array Read(Type type)
        {
            return Read(type, string.Empty, string.Empty);
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

    }

}
