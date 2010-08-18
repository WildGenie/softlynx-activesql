using System;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.IO;
using Softlynx.RecordCache;


namespace Softlynx.ActiveSQL
{
    public static class DateTimeFilter
    {
        /// <summary>
        /// Determine default DateTime represenation an all DB related DateTime fields
        /// </summary>
        public static DateTimeKind DBDefaultDateTimeKind=DateTimeKind.Local;

        internal static object FromDB(object o)
        {
            return FromDB(o, DateTimeKind.Unspecified);
        }

        /// <summary>
        /// Ajust input datetime object to DBDefaultDateTimeKind if its kind unspecified
        /// and return actial DateTime in Local kind
        /// </summary>
        /// <param name="o">any object</param>
        /// <returns>changed date time kind to DBDefaultDateTimeKind if unspecified and convert to Local DateTime</returns>
        internal static object FromDB(object o,DateTimeKind ForceTimeKind)
        {
            if (o is DateTime)
            {
                if (ForceTimeKind == DateTimeKind.Unspecified)
                    ForceTimeKind = DBDefaultDateTimeKind;

                if (ForceTimeKind == DateTimeKind.Unspecified)
                    return o;
                DateTime dt = (DateTime)o;
                if (dt.Kind == DateTimeKind.Unspecified)
                    dt = DateTime.SpecifyKind(dt, ForceTimeKind);
                return dt.ToLocalTime();
            }
            else
                return o;
        }

        internal static object ToDB(object o)
        {
            return ToDB(o, DateTimeKind.Unspecified);
        }

        /// <summary>
        /// Ajust DateTime value to DBDefaultDateTimeKind passed to DB backend
        /// </summary>
        /// <param name="o">any object</param>
        /// <returns>Ajust date time kind to be DBDefaultDateTimeKind</returns>
        internal static object ToDB(object o,DateTimeKind ForceTimeKind)
        {
            if (o is DateTime)
            {
                if (ForceTimeKind == DateTimeKind.Unspecified)
                    ForceTimeKind = DBDefaultDateTimeKind;


                if (ForceTimeKind == DateTimeKind.Unspecified)
                    return o;

                DateTime dt = (DateTime)o;
                if (dt.Kind != ForceTimeKind)
                {
                    if (ForceTimeKind == DateTimeKind.Local)
                        return dt.ToLocalTime();
                    else
                        return dt.ToUniversalTime();
                }
            }
            return o;
        }


    }

    internal class Limit : Condition
    {
        internal int _RecordCount = -1;
        internal Limit(int RecordCount)
        {
            _RecordCount = RecordCount;
        }
    }

    internal class OrderBy : Condition
    {
        internal string[] columns = null;
        internal Where.SortBy _order = Where.SortBy.Ascendant;


        internal OrderBy(string[] Columns, Where.SortBy order)
        {
            columns = Columns;
            _order = order;
        }
    }

    public class ConditionDefs
    {
        internal InTable _table = null;
        internal Condition[] conditions = null;
        internal RecordManager rm = null;

        internal int RecordLimit = -1;
        internal string WhereClause = null;
        internal string OrderClause = null;
        internal Hashtable ParameterValues = new Hashtable();


        /// <summary>
        /// Returns ready WHERE statement
        /// </summary>
        public string Where
        {
            get { return WhereClause; }
        }

        /// <summary>
        /// Returns ready ORDER BY statement
        /// </summary>
        public string OrderBy
        {
            get { return OrderClause; }
        }

        /// <summary>
        /// Returns key=value pairs for parsed conditional defines
        /// </summary>
        public Hashtable Parameters
        {
            get { return ParameterValues; }
        }
        private ConditionDefs(InTable tbl, Condition[] _conditions)
        {
            _table=tbl;
            InitInstance(tbl.manager,_conditions);
        }
        private ConditionDefs(RecordManager _rm, Condition[] _conditions)
        {
            InitInstance(_rm,_conditions);
        }

        private void InitInstance(RecordManager _rm, Condition[] _conditions)
        {
            conditions = _conditions;
            rm = _rm;
            List<string> OrderColumns = new List<string>();
            List<string> WhereConds = new List<string>();
            if (conditions!=null)
                foreach (Condition cond in conditions)
                {
                    if (cond is Limit)
                        RecordLimit = (cond as Limit)._RecordCount;

                    if (cond is OrderBy)
                    {
                        foreach (string clmn in (cond as OrderBy).columns)
                        {
                            InField fld=null;
                            if (_table != null)
                            {
                                fld = _table.Field(clmn);
                            } 
                            OrderColumns.Add(
                                ((fld!=null)?rm.AsFieldName(fld,ProviderSpecifics.StatementKind.OrderBy)
                                           :rm.AsFieldName(clmn)) + " " +
                                (((cond as OrderBy)._order) == Condition.Ascendant ? "ASC" : "DESC"));
                        }
                    }

                    if (cond is WhereCondition)
                        WhereConds.Add(BuildWhereCondExpr(cond as WhereCondition));
                }
            WhereClause = string.Join(" AND ", WhereConds.ToArray());
            OrderClause = string.Join(",", OrderColumns.ToArray());
        }

        private string BuildWhereCondExpr(WhereCondition whereCondition)
        {

            if (whereCondition is Where) 
            {
                Where w=whereCondition as Where;
                string pname = string.Format("PRM{0}", ParameterValues.Count);
                ParameterValues.Add(pname,w.value);

                InField fld = null;
                if (_table != null)
                {
                    fld = _table.Field(w.field);
                } 

                return string.Format("({0} {1} {2})",
                            ((fld != null) ? rm.AsFieldName(fld, ProviderSpecifics.StatementKind.Where)
                                           : rm.AsFieldName(w.field)),
                              w.op,
                              rm.AsFieldParam(pname)
                              );
            }

            WhereGroup g = whereCondition as WhereGroup;
            List<string> WhereConds = new List<string>();
            foreach (WhereCondition c in g.conditions)
                WhereConds.Add(BuildWhereCondExpr(c));
            return "(" + string.Join(" " + g.JoinOperator + " ", WhereConds.ToArray()) + ")";
        }


        public static ConditionDefs Parse(RecordManager rm, params Condition[] conditions)
        {
            return new ConditionDefs(rm, conditions);
        }

        public static ConditionDefs Parse(InTable table, params Condition[] conditions)
        {
            return new ConditionDefs(table, conditions);
        }

    }
    
    
    public abstract class Condition
    {
        public const Where.SortBy Descendant = Where.SortBy.Descendant;
        public const Where.SortBy Ascendant = Where.SortBy.Ascendant;

        public static Condition Limit(int RecordCount)
        {
            return new Limit(RecordCount);
        }

        public static Condition OrderBy(params string[] Columns)
        {

            return  OrderByAsc(Columns);
        }

        public static Condition OrderByAsc(params string[] Columns)
        {
            return new OrderBy(Columns, Where.SortBy.Ascendant);
        }

        public static Condition OrderByDesc(params string[] Columns)
        {

            return new OrderBy(Columns, Where.SortBy.Descendant);
        }
        public static Condition OrderBy(string Column, Where.SortBy order)
        {
            return new OrderBy(new string[] { Column }, order);
        }
    }

    abstract public class WhereCondition : Condition {
    }

    internal class WhereGroup : WhereCondition
    {
        internal string JoinOperator = null;
        internal WhereCondition[] conditions = null;
        internal WhereGroup(string Operator, params WhereCondition[] Conditions)
        {
            JoinOperator = Operator;
            conditions = Conditions;
        }
    

    }

    public class Where : WhereCondition
    {
        public enum SortBy { Ascendant, Descendant }

        internal string field = string.Empty;
        internal object value = null;
        internal string op = null;

        private Where() { }

        private Where(string FieldName, string Operator, object Value)
        {
            field = FieldName;
            value = Value;
            op = Operator;
        }

        public static WhereCondition AND(params WhereCondition[] Conditions)
        {
            return new WhereGroup("AND", Conditions);
        }

        public static WhereCondition OR(params WhereCondition[] Conditions)
        {
            return new WhereGroup("OR", Conditions);
        }

        public static WhereCondition OP(string FiledName, string Operator, object Value)
        {
            return new Where(FiledName, Operator, Value);
        }

        public static WhereCondition EQ(string FiledName, object Value)
        {
            return OP(FiledName, "=", Value);
        }

        public static WhereCondition NE(string FiledName, object Value)
        {
            return OP(FiledName, "!=", Value);
        }

        public static WhereCondition GE(string FiledName, object Value)
        {
            return OP(FiledName, ">=", Value);
        }

        public static WhereCondition GT(string FiledName, object Value)
        {
            return OP(FiledName, ">", Value);
        }

        public static WhereCondition LE(string FiledName, object Value)
        {
            return OP(FiledName, "<=", Value);
        }

        public static WhereCondition LT(string FiledName, object Value)
        {
            return OP(FiledName, "<", Value);
        }
    }

    public class NotActiveRecordException : ApplicationException { 
        public NotActiveRecordException(string msg):base(msg){}
    };

    public abstract class ProviderSpecifics
    {
        public enum StatementKind {Declaration,Where,OrderBy,Index};

        private Hashtable type_mappings = null;
        private DbConnection db_connection = null;
        
        private bool _AutoSchema=true;

        /// <summary>
        /// Determine will the record manager handle schema creation and versioning for that \
        /// specific connection 
        /// </summary>
        public bool AutoSchema
        {
            get { return _AutoSchema; }
            set { _AutoSchema = value; }
        }


        protected abstract Hashtable CreateTypeMapping();
        public abstract DbConnection CreateDbConnection();

        public virtual string SQL_SELECT(string columns,string tables,string where,string order_columns,int limit)
        {
            string cmd = "SELECT "+columns+" FROM "+tables;
           
            if ((where!=null) && (where != string.Empty))
            cmd+=" WHERE "+where;
            
            if ((order_columns!=null) && (order_columns != string.Empty))
                cmd += " ORDER BY " + order_columns;

            if (limit>0)
                cmd += " LIMIT " + limit.ToString();

            return cmd;
        }

        protected Hashtable TypeMappings
        {
            get
            {
                if (type_mappings==null)
                    type_mappings=CreateTypeMapping();
                return type_mappings;
            }
        }

        public abstract DbParameter CreateParameter(string name, Type t);
        public virtual DbParameter CreateParameter(string name, object value)
        {
            DbParameter res=CreateParameter(name, value.GetType());
            res.Value = value;
            return res;
        }
        public virtual DbParameter SetupParameter(DbParameter param, InField f)
        {
            return param;
        }

        public virtual string GetSqlType(InField f)
        {
            return GetSqlType(f.FieldType);
        }
        public virtual string GetSqlType(Type t)
        {
            object[] o = (object[])TypeMappings[t];
            if (t.IsEnum)
                o = (object[])TypeMappings[typeof(int)];
            if (o == null) o=(object[])TypeMappings[typeof(object)];
            return (string)o[0];
        }


        public virtual DbType GetDbType(Type t)
        {
            object[] o = (object[])TypeMappings[t];
            if (t.IsEnum)
                o = (object[])TypeMappings[typeof(int)];
            if (o == null) return DbType.Object;
            return (DbType)o[1];
        }

        public virtual string AsFieldName(InField s,StatementKind sk)
        {
            return AsFieldName(s.Name);
        }


        public abstract string AsFieldName(string s);
        public abstract string AsFieldParam(string s);
        public abstract string AutoincrementStatement(string ColumnName);

        public virtual string DropTableIfExists(string TableName)
        {
            return String.Format("DROP TABLE IF EXISTS {0};", AsFieldName(TableName));
        }
        
        public virtual DbConnection Connection
        {
            get
            {
                if (db_connection == null)
                    db_connection = CreateDbConnection();
                return db_connection;
            }
        }
        public abstract void ExtendConnectionString(string key, string value);
        
        public virtual string AdoptSelectCommand(string select, InField[] fields)
        {
            return select;
        }
    }


    #region Common attributes
    public enum TableAction { None, RunSQL, Recreate };
    public enum ColumnAction {Insert,Remove,ChangeType,Recreate};

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class ExcludeFromTable : Attribute { }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class Indexed : Attribute { }

    [AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
    public class Autoincrement : Attribute { }

    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class WithReplica : Attribute { }


    /// <summary>
    /// Instructs the record manager about database schema is not under framework control.
    /// In other case framework will not attempt to create reflected table schema 
    /// and raise an exception in case of TableVersion TableAction.Recreate or any ColumnAction
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class PredefinedSchema : Attribute { }

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
        private bool _HideInherited=false;

        public bool HideInheritedPK
        {   
            get { return _HideInherited; }
            set { _HideInherited=value; }
        }
	
    }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetInsert : NamedAttribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class RecordSetRemove : NamedAttribute { }

    [AttributeUsage(AttributeTargets.Method, Inherited = true, AllowMultiple = false)]
    public class OnTableVersionChange : Attribute {
        
        private bool _PostRegistration=false;
        private int? _version = null;

        /// <summary>
        /// True if handler will run asyncronously after all registration sequences
        /// </summary>
        public bool PostRegistration
        {
            get { return _PostRegistration; }
            set { _PostRegistration = value; }
        }

        public int? Version
        {
            get { return _version; }
            set { _version = value; }
        }

    }


    
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
        private string _column=null;
        private TableAction _table_action = TableAction.None;
        private ColumnAction _column_action = ColumnAction.Insert;

        public string SQLCode
        {
            get { return _sql_code; }
            set { _sql_code = value; }
        }

        public string ColumnName
        {
            get { return _column; }
            set { _column = value; }
        }

        public TableAction Action
        {
            get { return _table_action; }
            set { _table_action = value; }
        }

        public ColumnAction ColumnAction
        {
            get { return _column_action; }
            set { _column_action = value; }
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

        public TableVersion(int version, ColumnAction action, string column_name,string sql_code)
        {
            Version = version;
            ColumnAction = action;
            ColumnName = column_name;
            SQLCode = sql_code;
            Action = TableAction.RunSQL;
        }

        public TableVersion(int version, ColumnAction action, string column_name)
        {
            Version = version;
            ColumnAction = action;
            ColumnName = column_name;
            SQLCode = null;
            Action = TableAction.None;
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

        private DbType? _DBType=null;
        private DateTimeKind _DateKind = DateTimeKind.Unspecified;

        public bool Indexed
        {
            get { return IsIndexed; }
        }

        public bool Autoincrement
        {
            get { return IsAutoincrement; }
        }

        public DateTimeKind DateKind
        {
            get { return _DateKind; }
            set { _DateKind=value; }
        }

        public bool DBTypeDefined
        {
            get { return _DBType.HasValue; }
        }

        public DbType DBType
        {
            get { return DBTypeDefined?_DBType.Value:DbType.Object;}
            set { _DBType = value; }

        }
        public InField()
            : base()
        {
        }

        public InField(DbType dbt):base()
        {
            DBType = dbt;
        }
        internal PropertyInfo prop
        {
            get { return _prop; }
            set { 
             
                _prop=value;
            }
        }

        private byte _precision = 0;

        public byte Precision
        {
            get { return _precision; }
            set { _precision = value; }
        }

        private byte _scale = 0;
        public byte Scale
        {
            get { return _scale; }
            set { _scale = value; }
        }

        private int _size = 0;
        public int Size
        {
            get { return _size; }
            set { _size = value; }
        }

        internal string GetValue(object obj)
        {
            return ValueFormatter.Serialize(prop.GetValue(obj, null));
        }

        internal void SetValue(object obj,string v)
        {
            object ov=ValueFormatter.Deserialize(field_type,v);
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

        internal object PrepareValueType(object v)
        {
            if (v == null) return v;
            v = DateTimeFilter.FromDB(v,DateKind);
            if (prop.PropertyType.IsInstanceOfType(v)) return v;
            if (prop.PropertyType.IsEnum) 
                return Enum.ToObject(prop.PropertyType,v);
            return Convert.ChangeType(v, prop.PropertyType, null);
        }
    }


    #endregion
    
    public delegate void RecordManagerEvent(object o);
    public delegate void RecordManagerWriteEvent(object o, ref bool Handled);
    public delegate void RecordSetEvent(object o, object recordset);


  
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class InTable : NamedAttribute,ICloneable

    {
        internal bool _PredefinedSchema= false;

        /// <summary>
        /// Determines the database schema is not under the framework control and
        /// were defined outside the application.
        /// In case of true the framework will not attempt to create reflected table schema 
        /// and raise an exception in case of TableVersion TableAction.Recreate or any ColumnAction
        /// </summary>
        public bool PredefinedSchema
        {
            get { return _PredefinedSchema; }
        }

        internal RecordManager manager = null;

        private DbCommand InsertCmd = null;
        private DbCommand UpdateCmd = null;
        private DbCommand DeleteCmd = null;
        private DbCommand FillCmd = null;
        internal bool with_replica = false;

        private InField[] _fields;
        private Hashtable hf = new Hashtable();
        internal void RehashFields()
        {
            hf.Clear();
            foreach (InField f in _fields)
            {
                hf[f.Name] = f;
            }
        }
        internal InField[] fields
        {
            get { return _fields; }
            set { 
                _fields = value;
                RehashFields();
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
        public event RecordManagerEvent TableVersionAsyncChanged = null;
        public event RecordSetEvent RecordSetInsert = null;
        public event RecordSetEvent RecordSetRemove = null;
        
        public Type BaseType
        {
            get { return basetype; }
        }

        public bool WithReplica
        {
            get { return with_replica;}
        }

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
            return manager.specifics.DropTableIfExists(Name);
            
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
                if (f.IsAutoincrement) 
                    continue;
                
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
                if (dc.IsAutoincrement)
                    continue;

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
                if (!field.IsAutoincrement) 
                    InsertCmd.Parameters.Add(manager.CreateParameter(field));
                if ((!field.IsAutoincrement) || (field.IsPrimary))
                    UpdateCmd.Parameters.Add(manager.CreateParameter(field));
            }

            foreach (InField field in primary_fields)
            {
                DeleteCmd.Parameters.Add(manager.CreateParameter(field));
                FillCmd.Parameters.Add(manager.CreateParameter(field));
            }
            

            InsertCmd.Prepare();
            UpdateCmd.Prepare();
            DeleteCmd.Prepare();
            FillCmd.Prepare();

        }

        internal void FireAfterReadEvent(object record)
        {
            if (AfterRecordManagerRead != null)
                AfterRecordManagerRead(record);
        }
        internal virtual bool Read(object Record)
        {
            manager.ReopenConnection(FillCmd);
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
                        v = field.PrepareValueType(v);
                        field.prop.SetValue(Record, v, null);
                        i++;
                    }
                    res = true;
                }
                reader.Close();
            }
            if (Record is IRecordManagerDriven)
                (Record as IRecordManagerDriven).RecordManager = manager;
            if (res) FireAfterReadEvent(Record);

            return res;
        }


        internal virtual int Update(object Record)
        {
            manager.ReopenConnection(UpdateCmd);
            foreach (DbParameter prm in UpdateCmd.Parameters)
            {
                InField field = Field(prm.ParameterName);
                if (field != null)
                {
                    prm.Value = DateTimeFilter.ToDB(field.prop.GetValue(Record, null),field.DateKind);
                }
            }
            return UpdateCmd.ExecuteNonQuery();
        }

        internal virtual int Insert(object Record)
        {
            manager.ReopenConnection(InsertCmd);

            foreach (DbParameter prm in InsertCmd.Parameters)
            {
                InField field = Field(prm.ParameterName);
                if (field != null)
                {
                    prm.Value = DateTimeFilter.ToDB(field.prop.GetValue(Record, null),field.DateKind);
                }
            }

            return InsertCmd.ExecuteNonQuery();
        }

        internal virtual int Write(object Record,bool IgnoreHandledStatus)
        {
            using (ManagerTransaction t=manager.BeginTransaction())
            {
                if (Record is IRecordManagerDriven)
                    (Record as IRecordManagerDriven).RecordManager = manager;

                bool write_handled = false;
                int r = 0;
                if (BeforeRecordManagerWrite != null) BeforeRecordManagerWrite(Record, ref write_handled);
                if (!write_handled || IgnoreHandledStatus)
                {
                    r = Update(Record);
                    if (r == 0) r = Insert(Record);
                };
                if ((AfterRecordManagerWrite != null)  && (r>0))  AfterRecordManagerWrite(Record);
                if (r>0) manager.DoRecordWritten(Record);
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
                if (Record is IRecordManagerDriven)
                    (Record as IRecordManagerDriven).RecordManager = manager;

                if (BeforeRecordManagerDelete != null) BeforeRecordManagerDelete(Record);
                int i = 0;
                manager.ReopenConnection(DeleteCmd);
                foreach (InField field in primary_fields)
                {
                    DeleteCmd.Parameters[i++].Value = field.prop.GetValue(Record, null);
                }
                res = DeleteCmd.ExecuteNonQuery();
                if ((AfterRecordManagerDelete != null) && (res>0)) AfterRecordManagerDelete(Record);
                if (res > 0) manager.DoRecordDeleted(Record);
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
            if (TableVersionAsyncChanged != null)
                manager.VersionChangePostHandlers.Enqueue(new RecordManagerEvent(delegate
                {
                    TableVersionAsyncChanged(version);
                }));
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
        [InField(Size=512)]
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
                try
                {
                    manager.transaction.Commit();
                }
                catch { }
        }

        public void Rollback()
        {
            complete = true;
            if (manager.TransactionLevel==1)
                try
                {
                    manager.transaction.Rollback();
                }
                catch { }
        }

    }

    public delegate void RecordOperation(RecordManager Manager, Object obj);
    public delegate RecordManager RecordManagerProvider();

    public class RecordManager:IDisposable
    {
        /// <summary>
        /// Event handler called on RecordManager instance disposed
        /// </summary>
        public event EventHandler Disposed = null;

        /// <summary>
        /// Event handler called on RecordManager object were stored or updated to DB
        /// </summary>
        public event RecordOperation OnRecordWritten=null;

        /// <summary>
        /// Event handler called on RecordManager object were deleted from DB
        /// </summary>
        public event RecordOperation OnRecordDeleted=null;
        
        
        private CacheCollector cache = new CacheCollector();

        internal Queue<RecordManagerEvent> VersionChangePostHandlers = new Queue<RecordManagerEvent>();

        static Hashtable managers = new Hashtable();

        /// <summary>
        /// Return the collection of system wide RecordManagers
        /// defined for running threads.
        /// </summary>
        public static ICollection Managers
        {
            get
            {
                ArrayList list = new ArrayList();
                lock (managers.SyncRoot) {
                foreach (object o in managers.Values)
                    if (o is RecordManager) list.Add(o);
                }
                return list;
            }
        }

        public CacheCollector Cache
        {
            get { return cache; }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            
            if (Disposed != null) Disposed(this, null);
            lock (managers.SyncRoot)
            {
                object pkey = managers[this];
                if (pkey != null) managers.Remove(pkey);
                managers.Remove(this);
            }
            cache.Dispose();
            //Connection.Close();
            FlushConnectionPool();
        }

        /// <summary>
        /// Release all the resourses, close all DB connections
        /// </summary>
         ~RecordManager()
        {
            Dispose();
        }

        /// <summary>
        /// Release all the created connection used for interator loops.
        /// </summary>
        public void FlushConnectionPool()
        {
            foreach (DbConnection c in ConnectionPool.Values)
            {
                try
                {
                    c.Close();
                    c.Dispose();
                }
                catch
                { };
            }
            ConnectionPool.Clear();
        }

        internal void ReopenConnection(DbCommand cmd)
        {
            
            if ((transaction!=null) && (cmd.Connection == transaction.Connection))
                cmd.Transaction = transaction;
            else
                cmd.Transaction = null;
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

        private static RecordManagerProvider RM_Provider=null;
        /// <summary>
        /// Default thread specific RecordManager provider delegate for lazy binding.
        /// Once the delegate works and returns RecordManager instance it resets back to null
        /// like ProviderDelegate=null were called next.
        /// </summary>
        public static RecordManagerProvider ProviderDelegate
        {
            get {
                return RM_Provider;
               }
            set {
                RM_Provider = value;
            }
        }

        /// <summary>
        /// Default thread specific record manager.
        /// </summary>
        public static RecordManager Default
        {
            get
            {
                RecordManager _default=null;
                
                if (ProviderDelegate != null)
                    _default = ProviderDelegate();
                else
                    _default = (RecordManager)managers[Thread.CurrentThread.ManagedThreadId];

                if (_default == null)
                    throw new ApplicationException("Default Record Manager is not defined");
                return _default;
            }
            set
            {
                if (ProviderDelegate != null)
                    throw new ApplicationException("Can't set Default Record Manager while ProviderDelegate is defined.");

                lock (managers.SyncRoot)
                {
                    if (value == null)
                    {
                        object pkey = managers[Thread.CurrentThread.ManagedThreadId];
                        if (pkey!=null)  managers.Remove(pkey);
                        managers.Remove(Thread.CurrentThread.ManagedThreadId);
                    }
                    else
                    {
                        object pkey = managers[value];
                        if (pkey!=null) managers.Remove(pkey);
                        managers[value] = Thread.CurrentThread.ManagedThreadId;
                        managers[Thread.CurrentThread.ManagedThreadId] = value;
                    }
                }
            }
        }
        /// <summary>
        /// Check the default Record manager for current thread is defined.
        /// </summary>
        public static bool DefaultIsDefined
        {
            get
            {
                if (ProviderDelegate != null)
                    throw new ApplicationException("Can't determine is Default Record Manager defined when ProviderDelegate used.");
                return managers.ContainsKey(Thread.CurrentThread.ManagedThreadId);
            }
        }


        internal ProviderSpecifics specifics;
        internal DbTransaction transaction = null;
        internal int TransactionLevel=0;

        internal Hashtable ConnectionPool = new Hashtable();


        /// <summary>
        /// Get connection from pool or create a new one if pool is empty
        /// </summary>
        /// <returns></returns>
        public DbConnection PooledConnection
        {
            get
            {
                DbConnection res = null;
                lock (ConnectionPool)
                {
                    if (ConnectionPool.Count == 0)
                    {
                        res = (Connection as ICloneable).Clone() as DbConnection;
                    }
                    else
                    {
                        foreach (DbConnection c in ConnectionPool.Values)
                        {
                            res=c;
                            break;
                        }
                        ConnectionPool.Remove(res);
                    }
                    return res;
                }
            }
        }


        public DbConnection Connection
        { 
            get {
                    return  specifics.Connection;
                }
        }

        private Hashtable tables = new Hashtable();
        private Hashtable table_names = new Hashtable();

        internal void ClearRegistrations()
        {
            tables.Clear();
            table_names.Clear();
        }

        public RecordManager(ProviderSpecifics ProviderSpecifics, params Type[] RegisterTypes)
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
            return CreateCommand(false, Command);
        }

        public DbCommand CreateCommand(bool pooled,string Command)
        {
            return CreateCommand(pooled,Command, new object[] { });
        }

        public DbCommand CreateCommand(string command, params object[] parameters)
        {
            return CreateCommand(false, command, parameters);
        }

        public DbCommand CreateCommand(bool pooled,string command, params object[] parameters )
        {
            DbConnection conn = pooled ?  PooledConnection :  Connection;
            
            DbCommand cmd = conn.CreateCommand();
            ReopenConnection(cmd);

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
                cmd.Prepare();
            }
            
            return cmd;
        }

        public int RunCommand(string command, params object[] parameters)
        {
            using (DbCommand cmd = CreateCommand(command, parameters))
            {
                cmd.Transaction = transaction;
                return cmd.ExecuteNonQuery();
            }
        }

        public object RunScalarCommand(string command, params object[] parameters)
        {
            using (DbCommand cmd = CreateCommand(command, parameters))
                return cmd.ExecuteScalar();
        }
  
        internal class PooledDataReader : DbDataReader
        {
            DbDataReader r=null;
            RecordManager m = null;
            DbConnection c = null;

            internal event EventHandler OnClose = null;

            internal PooledDataReader(DbDataReader reader, DbConnection conn, RecordManager manager) { r = reader; c = conn; m = manager; }

            public override int Depth { get { return r.Depth; } }
            public override int FieldCount { get { return r.FieldCount; } }
            public override bool HasRows { get { return r.HasRows; } }
            public override bool IsClosed { get { return r.IsClosed; } }
            public override int RecordsAffected { get { return r.RecordsAffected; } }
            public override int VisibleFieldCount { get { return r.VisibleFieldCount; } }
            public override object this[int ordinal] { get { return r[ordinal]; } }
            public override object this[string name] { get { return r[name]; } }
            public override void Close() {
                r.Close();
                m.ReturnConnectionToPool(c);
                if (OnClose != null)
                {
                    OnClose(this, null);
                }
            }
            ~PooledDataReader()
            {
                r.Dispose();
                r = null;
            }
            public override bool GetBoolean(int ordinal) { return r.GetBoolean(ordinal); }
            public override byte GetByte(int ordinal) { return r.GetByte(ordinal); }
            public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
                { return r.GetBytes(ordinal,dataOffset,buffer,bufferOffset,length ); }
            public override char GetChar(int ordinal) { return r.GetChar(ordinal); }
            public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
            { return r.GetChars(ordinal,dataOffset,buffer,bufferOffset,length); }
            public new DbDataReader GetData(int ordinal) { NotImplemented(); return null; }
            public override string GetDataTypeName(int ordinal) { return r.GetDataTypeName(ordinal) ; }
            public override DateTime GetDateTime(int ordinal){ return r.GetDateTime(ordinal) ; }
            protected override DbDataReader GetDbDataReader(int ordinal) { NotImplemented(); return null; }
            public override decimal GetDecimal(int ordinal){ return r.GetDecimal(ordinal) ; }
            public override double GetDouble(int ordinal){ return r.GetDouble(ordinal) ; }
            public override IEnumerator GetEnumerator() { return (r as IEnumerable).GetEnumerator(); }
            public override Type GetFieldType(int ordinal) { return r.GetFieldType(ordinal) ; }
            public override float GetFloat(int ordinal) { return r.GetFloat(ordinal) ; }
            public override Guid GetGuid(int ordinal) { return r.GetGuid(ordinal) ; }
            public override short GetInt16(int ordinal){ return r.GetInt16(ordinal) ; }
            public override int GetInt32(int ordinal){ return r.GetInt32(ordinal) ; }
            public override long GetInt64(int ordinal){ return r.GetInt64(ordinal) ; }
            public override string GetName(int ordinal) { return r.GetName(ordinal) ; }
            public override int GetOrdinal(string name){ return r.GetOrdinal(name); }
            public override Type GetProviderSpecificFieldType(int ordinal){
                NotImplemented();
                return null;
            }
            public override object GetProviderSpecificValue(int ordinal) { NotImplemented(); return null; }
            public override int GetProviderSpecificValues(object[] values) { NotImplemented(); return 0; }
            public override DataTable GetSchemaTable() { return r.GetSchemaTable(); }
            public override string GetString(int ordinal) { return r.GetString(ordinal) ; }
            public override object GetValue(int ordinal) { return r.GetValue(ordinal) ; }
            public override int GetValues(object[] values) { return r.GetValues(values); }
            public override bool IsDBNull(int ordinal) { return r.IsDBNull(ordinal); }
            public override bool NextResult() { return r.NextResult(); }
            public override bool Read() { return r.Read(); }
            private void NotImplemented()
            {
                throw new Exception("The method or operation is not implemented.");
            }
        }

        public void ReturnConnectionToPool(DbConnection conn)
        {
            ConnectionPool.Add(conn, conn);
        }
    
        /// <summary>
        /// Create a reader instance running separate pooled connection
        /// </summary>
        /// <param name="command"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public DbDataReader CreateReader(string command, params object[] parameters)
        {
            DbCommand cmd = CreateCommand(true, command, parameters);
            DbConnection conn=cmd.Connection;
            return new PooledDataReader(cmd.ExecuteReader(),conn,this);
        }

        /// <summary>
        /// Enumerate over InTable objects
        /// </summary>
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
                    throw new Exception(string.Format("Table name {0} already used by ActiveRecord object {1}", table.Name, type.ToString()));

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
                            OnTableVersionChange otc=mattr as OnTableVersionChange;
                            if (otc.PostRegistration)
                                table.TableVersionAsyncChanged += new RecordManagerEvent(delegate(object o) { CallMethod(m, null, o); });
                            else
                                table.TableVersionChanged += new RecordManagerEvent(delegate(object o) { CallMethod(m, null, o); });
                        }

                    }
                }
                Type base_only = null;
                Hashtable DefinedFields = new Hashtable();
                foreach (PropertyInfo prop in type.GetProperties())
                {
                    if (prop.IsDefined(typeof(ExcludeFromTable), true)) continue;
                    InField field = (InField)Attribute.GetCustomAttribute(prop, typeof(InField));
                    if (field == null) field = new InField();
                    if (field.Name == string.Empty) field.Name = prop.Name;
                    field.IsPrimary = prop.IsDefined(typeof(PrimaryKey), true);
                    foreach (PrimaryKey pk in prop.GetCustomAttributes(typeof(PrimaryKey), true))
                    {
                        if (pk._GenerateSQL_PK == false) field.GenerateSQL_PK = false;
                        if ((pk.HideInheritedPK) && (base_only == null))
                            base_only = prop.DeclaringType;
                    }
                    field.IsAutoincrement = prop.IsDefined(typeof(Autoincrement), true);
                    field.field_type = prop.PropertyType;
                    field.prop = prop;
                    
                    if (field.IsPrimary && (base_only!=null) && (!prop.DeclaringType.Equals(base_only)))
                        continue;

                    field.IsIndexed = prop.IsDefined(typeof(Indexed), true);
                    
                    if (DefinedFields.ContainsKey(field.Name))
                        continue;
                    
                    if (prop.CanWrite)
                    {
                        fields.Add(field);
                        DefinedFields[field.Name] = true;
                    }

                    if (field.IsPrimary)
                    {
                        if (primary_fields.Count > 0)
                            throw new Exception(string.Format("Can't define more than one field for primary index on object {0} ", type.ToString()));
                        primary_fields.Add(field);
                        DefinedFields[field.Name] = true;
                    }

                }
                table.with_replica = type.IsDefined(typeof(WithReplica), true);
                table._PredefinedSchema = type.IsDefined(typeof(PredefinedSchema), true);
                if ((table.with_replica) && (table.IsVirtual))
                {
                    throw new Exception(string.Format("Replica is not supported on virtual table {0}", table.Name));
                }

                table.fields = fields.ToArray();
                table.primary_fields = primary_fields.ToArray();
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
                        tables[type] = table;
                        table_names[table.Name] = type;
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
                    if (!table.PredefinedSchema && specifics.AutoSchema)
                        using (ManagerTransaction transaction = BeginTransaction())
                        {
                        attrs.Insert(0, new TableVersion(0, TableAction.Recreate));
                        attrs.Sort();
                        bool recreate_happened = false;
                        foreach (TableVersion update in attrs)
                        {
                            if (update.Version > ov.Version)
                            {
                                try
                                {
                                    if (update.Action == TableAction.Recreate)
                                    {
                                        if (table.PredefinedSchema)
                                            throw new ApplicationException("Cant't recreate table " + table.Name + " with PersistentSchema attribute active");
                                        string s = update.SQLCode;
                                        update.SQLCode = table.DropTableStatement();
                                        RunCommand(update.SQLCode);
                                        update.SQLCode = table.CreateTableStatement();
                                        RunCommand(update.SQLCode);
                                        update.SQLCode = s;
                                        recreate_happened = true;
                                    }
                                    if ((update.ColumnName != null) && (!recreate_happened))
                                    {
                                        if (table.PredefinedSchema)
                                            throw new ApplicationException("Cant't alter columns on table " + table.Name + " with PersistentSchema attribute active");
                                        InField colf = table.Field(update.ColumnName);
                                        if ((colf == null) && (update.ColumnAction != ColumnAction.Remove))
                                            throw new ApplicationException("Update reference not existing column " + update.ColumnName);
                                        string code = "ALTER TABLE " + AsFieldName(table.Name);
                                        switch (update.ColumnAction)
                                        {
                                            case ColumnAction.Remove:
                                                code += " DROP COLUMN " + AsFieldName(update.ColumnName);
                                                break;

                                            case ColumnAction.Recreate:
                                                code += " DROP COLUMN " + AsFieldName(colf.Name);
                                                code += ", ";
                                                code += " ADD COLUMN " + AsFieldName(colf.Name) + SqlType(colf);
                                                break;

                                            case ColumnAction.Insert:
                                                code += " ADD COLUMN " + AsFieldName(colf.Name) + SqlType(colf);
                                                break;

                                            case ColumnAction.ChangeType:
                                                code += " ALTER COLUMN " + AsFieldName(colf.Name) + " TYPE " + SqlType(colf);
                                                break;

                                            default: code = null;
                                                break;
                                        }
                                        if (code != null)
                                        {
                                            string s = update.SQLCode;
                                            update.SQLCode = code;
                                            RunCommand(code);
                                            update.SQLCode = s;
                                        }
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
                            transaction.Commit();
                        }
                    table.InitSqlMethods();
                    tables[type] = table;
                    table_names[table.Name] = type;
                    table.RehashFields();
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
            try
            {
                m.Invoke(o, prm);
            }
            catch (Exception E)
            {
                throw E.InnerException??E;
            }

                foreach (DictionaryEntry de in backref)
                    prms[(int)de.Value]=prm[(int)de.Key];
        }

        internal void InitStructure(params Type[] types)
        {
            if (specifics.AutoSchema)
            {
                foreach (Type t in types)
                {
                    if (!t.IsDefined(typeof(PredefinedSchema), true) && t.IsDefined(typeof(InTable), true))
                    {
                        TryToRegisterAsActiveRecord(typeof(ObjectVersions));
                        break;
                    }
                }
            }

                foreach (Type t in types)
                {
                    TryToRegisterAsActiveRecord(t);
                }

            using (ManagerTransaction transaction = BeginTransaction())
            {
                foreach (InTable t in tables.Values)
                    t.PostRegistrationEvent();
                
                while (VersionChangePostHandlers.Count>0) {
                    VersionChangePostHandlers.Dequeue()(null);
                };
                transaction.Commit();
            }

        }
        /// <summary>
        /// Reads an object from a adatabase
        /// </summary>
        /// <param name="Record">ActiveRecord object instance</param>
        /// <returns>Was it actualy found and readed</returns>
        public bool Read(Object Record)
        {
            return ActiveRecordInfo(Record.GetType()).Read(Record);
        }

        /// <summary>
        /// Writes an object to database
        /// </summary>
        /// <param name="Record">ActiveRecord object instance</param>
        /// <returns>Number of write operations. It could be zero in case of object reports it state does not require write operation.</returns>
        public int Write(Object Record)
        {
            return Write(Record, false);
        }

        /// <summary>
        /// Writes an object to database
        /// </summary>
        /// <param name="Record">ActiveRecord object instance</param>
        /// <param name="IgnoreHandledStatus">Instructs the kernel to ignore the object information about it's write requirement state in force write it any way</param>
        /// <returns>Number of write operations was made.</returns>
        public int Write(Object Record, bool IgnoreHandledStatus)
        {
            return ActiveRecordInfo(Record.GetType()).Write(Record, IgnoreHandledStatus);
        }

        internal void DoRecordWritten(object Record)
        {
            if (OnRecordWritten != null)
                OnRecordWritten(this, Record);
        }

        internal void DoRecordDeleted(object Record)
        {
            if (OnRecordDeleted != null) 
                OnRecordDeleted(this, Record);
        }

        /// <summary>
        /// Deletes the object form database
        /// </summary>
        /// <param name="Record">ActiveRecord object instance</param>
        /// <returns>Number of deleted records</returns>
        public int Delete(Object Record)
        {
            return ActiveRecordInfo(Record.GetType()).Delete(Record);
        }
        /// <summary>
        /// Сравнивает два объекта на равенства по полям отражаемым в ActiveRecord
        /// </summary>
        /// <param name="a">Первый объект</param>
        /// <param name="b">Второй объект</param>
        /// <returns></returns>
        public bool Equal(Object a,Object b)
        {
            if (a.GetType() != b.GetType()) return false;
            InTable table = ActiveRecordInfo(a.GetType());
            bool res = true;
            foreach (InField f in table.fields)
            {
                object av = f.prop.GetValue(a, null);
                object bv = f.prop.GetValue(b, null);

                if (av == bv) continue;
                if ((av==null) || (bv==null)) {res = false; break;}
                if (!av.Equals(bv))
                {
                    res = false;
                    break;
                }
            }
            return res;
        }



        internal object PKEY(object Record)
        {
            InTable table = ActiveRecordInfo(Record.GetType());
            return table.PKEYValue(Record);
        }

        public InTable ActiveRecordInfo(Type type, bool withexception)
        {
            InTable table = (InTable)tables[type];
            if ((table == null) && (withexception)) throw new NotActiveRecordException(string.Format("Can't use {0} as Acive Record object", type.Name));
            return table;
        }

        public InTable ActiveRecordInfo(Type type)
        {
            return ActiveRecordInfo(type, true);
        }
 
        public string SqlType(InField f) {
            return specifics.GetSqlType(f);
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

        public DbParameter CreateParameter(InField f)
        {
            DbParameter res=specifics.CreateParameter(f.Name, f.FieldType);
            if (f.DBTypeDefined)
                res.DbType = (DbType)f.DBType;
            return specifics.SetupParameter(res,f);
        }

        public DbParameter CreateParameter(string name, Type type)
        {
            return specifics.CreateParameter(name, type);
        }

        public string AsFieldName(InField f,ProviderSpecifics.StatementKind sf)
        {
            return specifics.AsFieldName(f,sf);
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
            return string.Format("{0} {1} {2}", AsFieldName(field), operation, AsFieldParam(field));
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
