using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Reflection;
using Softlynx.ActiveSQL;

namespace Softlynx.RecordSet
{
   
    public class RecordIterator : IEnumerable, IEnumerator, IDisposable
    {
       // private ManagerTransaction transaction = null;
        private InTable table = null;
        private string cmd = string.Empty;
        private object[] filter_params = null;
        private DbDataReader reader = null;
        private object[] cparams = null;
        private ConstructorInfo ci = null;
        

        internal RecordIterator(InTable _table, Type[] ptypes, string filter, string orderby, int limit, params object[] _filter_params)
        {
            table = _table;
            if (table.IsVirtual)
                throw new Exception("Can't fill virual tables from database");

            filter_params = _filter_params;

            cmd += String.Format("SELECT {0} from {1}", table.ColumnsList(table.fields), table.manager.AsFieldName(table.Name));
            if (filter != string.Empty)
            {
                cmd += String.Format(" WHERE ({0})", filter);
            };

            if (orderby != string.Empty)
            {
                cmd += String.Format(" ORDER BY {0}", orderby);
            };


            if (limit > 0)
            {
                cmd += String.Format(" LIMIT {0}", limit);
            };

            cparams = new object[] { table.manager };
            ci = table.basetype.GetConstructor(new Type[] { typeof(RecordManager) });
            if (ci == null)
            {
                cparams = null;
                ci = table.basetype.GetConstructor(new Type[] { });
            };

            if (ci == null)
                throw new Exception(string.Format("Can't create instance of {0} without known constructor", table.basetype.Name));
        }

        public void Reset()
        {
            if (reader != null)
                reader.Close();
                reader = null;
        }

        public object Current
        {
            get
            {
                object instance = ci.Invoke(cparams);
                if ((cparams == null) && (instance is IRecordManagerDriven))
                {
                    (instance as IRecordManagerDriven).Manager = table.manager;
                }
                int i = 0;
                foreach (InField field in table.fields)
                {
                    Object v = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    i++;
                    Type pt = field.prop.PropertyType;
                    object nv = v;
                    if ((!pt.IsPrimitive) && (pt.Namespace!="System")) {
                        ConstructorInfo nvci=pt.GetConstructor(new Type[] { v.GetType() });
                        if (nvci != null)
                            nv = nvci.Invoke(new object[] { v });
                    }
                    field.prop.SetValue(instance, nv, null);
                }

                table.FireAfterReadEvent(instance);
                    
                return instance;
            }
        }

        public bool MoveNext()
        {

            if (reader == null)
            {
                    reader = table.manager.CreateReader(cmd, filter_params);
            }
            return reader.Read();
        }

        public void Dispose()
        {
            Reset();
        }

        public IEnumerator GetEnumerator()
        {
            return this;
        }

        public int Fill(IList list)
        {
            int count = 0;
            foreach (object o in this)
            {
                list.Add(o);
                count++;
            }
            return count;
        }

        static public RecordIterator Enum(Type T,RecordManager manager, Type[] ptypes, string filter, string orderby, int limit, params object[] filter_params)
        {
            InTable t = manager.ActiveRecordInfo(T);
            return new RecordIterator(t, ptypes, filter, orderby, limit, filter_params);
        }

        static public RecordIterator Enum<T>(RecordManager manager, Type[] ptypes, string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum(typeof(T), manager, ptypes, filter, orderby, limit, filter_params);
        }


        static public RecordIterator Enum<T>(Type[] ptypes, string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum<T>(RecordManager.Default, ptypes, filter, orderby, limit, filter_params);

        }
        static public RecordIterator Enum(Type T,Type[] ptypes, string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum(T,RecordManager.Default, ptypes, filter, orderby, limit, filter_params);
        }


        static public RecordIterator Enum<T>(RecordManager manager, string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum<T>(manager, (Type[])null, filter, orderby, limit, filter_params);
        }

        static public RecordIterator Enum(Type T,RecordManager manager, string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum(T, manager, (Type[])null, filter, orderby, limit, filter_params);
        }

        static public RecordIterator Enum<T>(string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum<T>(RecordManager.Default, (Type[])null, filter, orderby, limit, filter_params);
        }

        static public RecordIterator Enum(Type T,string filter, string orderby, int limit, params object[] filter_params)
        {
            return Enum(T, RecordManager.Default, (Type[])null, filter, orderby, limit, filter_params);
        }

        static public RecordIterator Enum<T>(RecordManager manager, string filter, string orderby, params object[] filter_params)
        {
            return Enum<T>(manager, filter, orderby, 0, filter_params);
        }

        static public RecordIterator Enum(Type T,RecordManager manager, string filter, string orderby, params object[] filter_params)
        {
            return Enum(T,manager, filter, orderby, 0, filter_params);
        }

        static public RecordIterator Enum<T>(string filter, string orderby, params object[] filter_params)
        {
            return Enum<T>(RecordManager.Default, filter, orderby, 0, filter_params);
        }

        static public RecordIterator Enum(Type T,string filter, string orderby, params object[] filter_params)
        {
            return Enum(T,RecordManager.Default, filter, orderby, 0, filter_params);
        }

        static public RecordIterator Enum<T>(RecordManager manager, string filter, params object[] filter_params)
        {
            return Enum<T>(manager, filter, string.Empty, filter_params);
        }

        static public RecordIterator Enum(Type T,RecordManager manager, string filter, params object[] filter_params)
        {
            return Enum(T,manager, filter, string.Empty, filter_params);
        }

        static public RecordIterator Enum<T>(string filter, params object[] filter_params)
        {
            return Enum<T>(RecordManager.Default, filter, string.Empty, filter_params);
        }

        static public RecordIterator Enum(Type T,string filter, params object[] filter_params)
        {
            return Enum(T,RecordManager.Default, filter, string.Empty, filter_params);
        }

        static public RecordIterator Enum<T>(RecordManager manager)
        {
            return Enum<T>(manager, string.Empty, string.Empty);
        }

        static public RecordIterator Enum(Type T,RecordManager manager)
        {
            return Enum(T,manager, string.Empty, string.Empty);
        }

        static public RecordIterator Enum(Type T)
        {
            return Enum(T,RecordManager.Default);
        }

        static public RecordIterator Enum<T>()
        {
            return Enum<T>(RecordManager.Default);
        }



    }

   public class RecordSet<T>:IEnumerable,ICollection,IList,IDisposable
    {
        private Hashtable index = new Hashtable();
        private List<T> list = new List<T>();
        private InTable table = null;
        private static System.Type[] EmptyTypes = new System.Type[0];

        public void Clear()
        {
            lock (this)
            {
                index.Clear();
                list.Clear();
            }
        }

        public void PopulateTargetList(IList targetlist)
        {
            foreach (object o in list)
            {
                targetlist.Add(o);
            }
        }

        public void Dispose()
        {
            table = null;
            Clear();
        }

        public int Count { get { lock (this) return list.Count; } }

        public void CopyTo(Array array, int index)
        {
            lock (this)
            list.CopyTo((T[])array, index);
        }

        public Object SyncRoot { get { return this; } }

        public bool IsSynchronized { get {return true;}}

        public delegate void WhenAddNew(T record);
        public delegate void WhenRemoveExisting(T record);

        public event WhenAddNew OnInsert;
        public event WhenRemoveExisting OnRemoved;


        public RecordSet()
        {
            table = RecordManager.Default.ActiveRecordInfo(typeof(T));
        }
        
       public RecordSet(RecordManager Manager)
        {
            table=Manager.ActiveRecordInfo(typeof(T));
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
                    if (OnInsert != null) OnInsert(record);
                    table.DoInsertToRecordSet(record, this);
                }
            return record;
        }

        public List<T> ClonedList()
        {
            lock (this)
            {
                return new List<T>(list);
            }
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
                    {
                        if (OnRemoved != null) OnRemoved(record);
                        table.DoRemoveFromRecordSet(record, this);
                    }

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
                        o = table.basetype.GetConstructor(EmptyTypes).Invoke(null);
                        table.SetPKEYValue(o, key);
                        if (table.Read(o)) Add((T)o); else o=null;
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

        private bool _clearBeforeFill=true;

        public bool ClearBeforeFill
        {
            get { return _clearBeforeFill; }
            set { _clearBeforeFill = value; }
        }

       
       public void Fill(Type[] ptypes,string filter, string orderby, int limit, params object[] filter_params)
        {
           if (ClearBeforeFill)
           {
            list.Clear();
            index.Clear();
           }

           foreach (T instance in RecordIterator.Enum<T>(table.manager,ptypes,filter,orderby,limit,filter_params))
                   Add(instance);
       }

       public void Fill(string filter, string orderby, int limit, params object[] filter_params)
       {
           Fill((Type[])null, filter, orderby, limit, filter_params);
       }

       public void Fill(string filter, string orderby, params object[] filter_params)
        {
            Fill(filter, orderby, 0, filter_params);
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

       
    }
    
}
