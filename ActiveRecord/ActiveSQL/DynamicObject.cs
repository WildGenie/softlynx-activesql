using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Collections.Generic;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using System.Reflection;

namespace Softlynx.ActiveSQL
{

    public interface IIDObject
    {
        Guid ID { get; set;}
    }

    public class IDObject:IIDObject
    {
        private Guid _ID;

        [PrimaryKey]
        public Guid ID
        {
            get { return _ID; }
            set { _ID = value; }
        }
    }


    /// <summary>
    /// Базовый класс для любого объекта который должен хранить историю измения его свойств
    /// </summary>
    public interface IRecordManagerDriven
    {
        RecordManager Manager
        {
            get;
            set; 
        }
    }

    public interface IDynamicObject
    {
        bool IsNewObject
        {
            get;
        }

        bool HasChanges
        {
            get;
        }

        ObjectProp GetPropertyWithLastValue(PropType PropertyID);
        bool SetPropertyLastValue(PropType PropertyID, object value);

    }

    public enum PropertyMatch { Exact = 0, IgnoreCase = 1, Pattern = 2 }  ;

    public class PropertyIterator : IEnumerable, IEnumerator, IDisposable
    {
        RecordManager manager = null;
        private IEnumerator records=null;
        private Hashtable tracking = new Hashtable();
        private object cur = null;
        internal Type returnobjtype = null;

        internal PropertyIterator(IEnumerator Records)
        {
            records = Records;
            if (records is IRecordManagerDriven)
            {
                manager = (records as IRecordManagerDriven).Manager;
            }
            else
                manager = RecordManager.Default;
        }

        public void Dispose()
        {
            if (records is IDisposable)
            {
                (records as IDisposable).Dispose();
            }
            tracking.Clear();
            tracking = null;
            records = null;
        }

        public void Reset()
        {
            records.Reset();
            tracking.Clear();
        }

        public object Current
        {
            get
            {
                return cur;
            }
        }

        public bool MoveNext()
        {
            while (records.MoveNext())
            {
               cur = records.Current;
               ObjectProp op = cur as ObjectProp;

               
                    if ((returnobjtype != null) && (op!=null))
                    {
                        IDynamicObject o = Activator.CreateInstance(returnobjtype) as IDynamicObject;
                        if (o is IRecordManagerDriven)
                            (o as IRecordManagerDriven).Manager = manager;
                        if (o is IIDObject)
                            (o as IIDObject).ID = op.ObjectID;

                        if (!manager.Read(o)) continue;
                        if (o.GetPropertyWithLastValue(op.Property).ID!=op.ID) continue;
                        cur = o;
                    }
                return true;
            }
            return false;
        }
        
        public IEnumerator GetEnumerator()
        {
            return this;
        }

        public PropertyIterator For(Type t)
        {
            returnobjtype = t;
            return this;
        }

        public PropertyIterator For<T>()
        {
            return For(typeof(T));
        }


    }
    abstract public class DynamicObject : IIDObject, IComparable, IDynamicObject
    {
        protected RecordManager _manager;
        protected Guid _id = Guid.Empty;
        protected bool _isnewobject = true;

        protected Dictionary<PropType, ObjectProp> changed_values = new Dictionary<PropType, ObjectProp>();
        protected Dictionary<PropType, ObjectProp> props_latest_value = new Dictionary<PropType, ObjectProp>();

        int IComparable.CompareTo(object other)
        {
            return ToString().CompareTo(other.ToString());
        }


        [ExcludeFromTable]
        public bool IsNewObject
        {
            get { return _isnewobject; }
            set { _isnewobject = value; }
        }

        [ExcludeFromTable]
        public RecordManager Manager
        {
            get { return _manager; }
            set { _manager = value; }
        }

        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set
            {
                if (_id != value)
                {
                    _id = value;
                    Dictionary<PropType, ObjectProp> platest = changed_values;// props_latest_value;
                    changed_values = new Dictionary<PropType, ObjectProp>();
                    props_latest_value = new Dictionary<PropType, ObjectProp>();
                    foreach (KeyValuePair<PropType, ObjectProp> kv in platest)
                    {
                        SetPropertyLastValue(
                            kv.Key,
                            kv.Value.Value
                            );
                    }
                    platest.Clear();
                }
            }
        }

        /// <summary>
        /// Возвращает признак ниличия изменеий в свойствах объекта
        /// </summary>
        public bool HasChanges
        {
            get { return changed_values.Values.Count > 0; }
        }

        [AfterRecordManagerRead]
        protected void WhenRead()
        {
            IsNewObject = false;
        }

        public abstract ObjectProp GetPropertyWithLastValue(PropType PropertyID);
        public abstract bool SetPropertyLastValue(PropType PropertyID, object value);

    }
    abstract public class DynamicObject<T> : DynamicObject,   IRecordManagerDriven
    {

        static internal ConstructorInfo GetDynamicPropertyStorageClass()
            {
                Type PropertyStorageType=typeof(T);
                if (!typeof(ObjectProp).IsAssignableFrom(PropertyStorageType))
                    throw new Exception("Can not use class " + PropertyStorageType.FullName + " as property storage object. Inherit it from ObjectProp.");

                ConstructorInfo constr = PropertyStorageType.GetConstructor(new Type[] { });
                if (constr == null)
                    throw new Exception(PropertyStorageType.FullName + " does not implement parameterless constructoras property storage object.");
                return constr;
            }

        static private ConstructorInfo ObjPropConstructor = null;

        public DynamicObject()
        {
        if (ObjPropConstructor==null) 
        ObjPropConstructor=GetDynamicPropertyStorageClass();
    if ((Manager == null) && (RecordManager.DefaultIsDefined))
        Manager = RecordManager.Default;
        }

        public DynamicObject(RecordManager manager):this()
        {
            if (manager!=null)
                Manager = manager;
        }
        private ObjectProp PropInstance(Guid ObjectID, PropType PropertyID, object value)
        {
            ObjectProp res = ObjPropConstructor.Invoke(null) as ObjectProp;
            res.ObjectID = ID;
            res.PropertyID = PropertyID.ID;
            res.Value = value;
            return res;
        }
       

        [BeforeRecordManagerWrite]
        protected void ActiveRecordWrite(RecordManager manager,ref bool handled)
        {
            if (!handled)
            {
                foreach (ObjectProp op in changed_values.Values)
                {
                    manager.Write(op);
                }
                changed_values.Clear();
                handled=!IsNewObject;
            }
        }


        /// <summary>
        /// Получить модель описывающую для объекта последнее значение свойства с типом 
        /// PropertyID
        /// </summary>
        /// <param name="PropertyID">Тип запрашиваемого свойства</param>
        /// <returns>Модель T из базы либо вновь созданная модель</returns>
        public override ObjectProp GetPropertyWithLastValue(PropType PropertyID)
        {
            ObjectProp r = null;
            try
            {
                r = props_latest_value[PropertyID];
            }
            catch (KeyNotFoundException)
            {
                if (ID != Guid.Empty)
                {

                    foreach (ObjectProp op in RecordIterator.Enum<T>(Manager,
                            Manager.WhereEqual("ObjectID") +
                           " and " +
                            Manager.WhereEqual("PropertyID"),
                            string.Format("{0} DESC", Manager.AsFieldName("Created")),
                            "ObjectID", ID,
                            "PropertyID", PropertyID.ID))
                    {
                        r = op;
                        break;
                    }
                }

                if (r == null) r = PropInstance(ID, PropertyID, null);
                    else
                    IsNewObject = false;
                props_latest_value[PropertyID] = r;
            }
            return r;
        }

        /// <summary>
        /// Устанавливает последнее значение свойства объекта
        /// </summary>
        /// <param name="PropertyID">Тип устанавливаемого свойства</param>
        /// <param name="value">Новое значение</param>
        /// <returns>Признак того поменялось ли значение свойства объекта</returns>
        public override bool SetPropertyLastValue(PropType PropertyID, object value)
        {
            bool changed = false;
            ObjectProp r = GetPropertyWithLastValue(PropertyID);
            if (
                ((r.Value != null) && (value != null) && (!value.Equals(r.Value)))
                ||
                ((r.Value == null) && (value != null))
                ||
                ((r.Value != null) && (value == null))
                )
            {
                changed_values[PropertyID] = PropInstance(ID, PropertyID, value);
                changed = true;
                r.Value = value;
            }

            return changed;
        }

       

        [AfterRecordManagerRead]
        void ReadObject(RecordManager manager)
        {
            Manager = manager;
        }

        /// <summary>
        /// Возвращает текущее значене свойства объекта
        /// </summary>
        /// <param name="PropertyID">Тип запрашиваемого свойства</param>
        /// <returns>Текущее значение свойства</returns>
        public Object GetPropertyLastValue(PropType PropertyID)
        {
            return GetPropertyWithLastValue(PropertyID).Value;
        }

        public static PropertyIterator Query(RecordManager Manager, DynamicObject obj)
        {
            RecordIterator ri = RecordIterator.Enum<T>(Manager,
            Manager.WhereEqual("ObjectID"),
            string.Format("{0} DESC", Manager.AsFieldName("Created")),
            "ObjectID", obj.ID);
            return new PropertyIterator(ri);
        }

        public static PropertyIterator Query(DynamicObject obj)
        {
            return Query(RecordManager.Default, obj);
        }


        public static PropertyIterator Query(RecordManager Manager, PropType PropertyID, Object value, string  matching)
        {
            string cmpop = matching;

            string v = ValueFormatter.Serialize(value);

            /*
            if ((matching & PropertyMatch.Pattern) != 0)
            {
                if ((matching & PropertyMatch.IgnoreCase) != 0)
                cmpop = " ILIKE ";
                else
                cmpop = " LIKE ";
            }
            */

            RecordIterator ri=RecordIterator.Enum<T>(Manager,
            Manager.WhereEqual("PropertyID")+" and "+Manager.WhereExpression("ValueText",cmpop),
            string.Format("{0} DESC", Manager.AsFieldName("Created")),
            "PropertyID", PropertyID.ID,
            "ValueText", v);
            return new PropertyIterator(ri);
        }

        public static PropertyIterator Query(RecordManager Manager, PropType PropertyID, Object value)
        {
            return Query(Manager, PropertyID, value, "=");
        }

        public static IEnumerable Query<QT>(RecordManager Manager, PropType PropertyID, Object value, string matching)
        {
            return Query(Manager, PropertyID, value, matching).For<QT>();
        }

        public static IEnumerable Query<QT>(PropType PropertyID, Object value, string matching)
        {
            return Query(PropertyID, value, matching).For<QT>();
        }

        public static IEnumerable Query<QT>(RecordManager Manager, PropType PropertyID, Object value)
        {
            return Query(Manager, PropertyID, value).For<QT>();
        }


        public static PropertyIterator Query(PropType PropertyID, Object value)
        {
            return Query(RecordManager.Default,PropertyID, value);
        }

        public static IEnumerable Query<QT>(PropType PropertyID, Object value)
        {
            return Query(PropertyID, value).For<QT>();
        }


        public static PropertyIterator Query(PropType PropertyID, Object value, string matching)
        {
            return Query(RecordManager.Default, PropertyID, value, matching);
        }

        public static LT Locate<LT>(RecordManager Manager, PropType PropertyID, Object value, string matching)
        {
            LT r = default(LT);
            foreach (LT o in Query<LT>(Manager, PropertyID, value,matching))
            {
                r = o;
                break;
            }
            return r;
        }

        public static LT Locate<LT>(PropType PropertyID, Object value, string matching)
        {
            return Locate<LT>(RecordManager.Default, PropertyID, value, matching);
        }


        public static LT Locate<LT>(RecordManager Manager, PropType PropertyID, Object value)
        {
            return Locate<LT>(Manager, PropertyID, value, "=");
        }

        public static LT Locate<LT>(PropType PropertyID, Object value)
        {
            return Locate<LT>(RecordManager.Default, PropertyID, value);
        }
       
    }
}