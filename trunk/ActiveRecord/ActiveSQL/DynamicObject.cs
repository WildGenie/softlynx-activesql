using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Collections.Generic;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using Softlynx.ActiveSQL.Replication;
using System.Reflection;

namespace Softlynx.ActiveSQL
{


    /// <summary>
    /// Базовый класс для любого объекта который должен хранить историю измения его свойств
    /// </summary>
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
                manager = (records as IRecordManagerDriven).RecordManager;
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
                            (o as IRecordManagerDriven).RecordManager = manager;
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
    abstract public class DynamicObject : IDObject, IComparable, IDynamicObject,ISmartActiveRecord
    {
        protected RecordManager _manager;
//        protected bool _isnewobject = true;

//        protected Dictionary<PropType, ObjectProp> changed_values = new Dictionary<PropType, ObjectProp>();
//        protected Dictionary<PropType, ObjectProp> props_latest_value = new Dictionary<PropType, ObjectProp>();

        int IComparable.CompareTo(object other)
        {
            return ToString().CompareTo(other.ToString());
        }
        

        [ExcludeFromTable]
        public RecordManager Manager
        {
            get { return _manager; }
            set { _manager = value; }
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
            if (RecordManager.ProviderDelegate!=null)
                Manager=RecordManager.Default;
            else 
                if (RecordManager.DefaultIsDefined)
                    Manager = RecordManager.Default;
        }

        public DynamicObject(RecordManager manager)
        {
            if (ObjPropConstructor == null)
                ObjPropConstructor = GetDynamicPropertyStorageClass();

            if (manager!=null)
                Manager = manager;
        }

        private ObjectProp PropInstance(Guid ObjectID, PropType PropertyID, object value)
        {
            if (PropertyID.Anonymous)
                throw new ApplicationException("Can't use Anonymous property " + PropertyID.ToString() + " with DynamicObjects.");

            ObjectProp res = ObjPropConstructor.Invoke(null) as ObjectProp;
            res.ObjectID = ID;
            res.PropertyID = PropertyID.ID;
            res.Value = value;
            return res;
        }
       
        /// <summary>
        /// Получить модель описывающую для объекта последнее значение свойства с типом 
        /// PropertyID
        /// </summary>
        /// <param name="PropertyID">Тип запрашиваемого свойства</param>
        /// <returns>Модель T из базы либо вновь созданная модель</returns>
        public override ObjectProp GetPropertyWithLastValue(PropType PropertyID)
        {
            bool dbrequest = false;
            try
            {
                return GetValue<ObjectProp>(PropertyID, new DefaultValueDelegate<ObjectProp>(delegate
                {
                    dbrequest = true;
                    ObjectProp r = null;
                    if (ID != Guid.Empty)
                    {

                        foreach (ObjectProp op in RecordIterator.Enum<T>(Manager,
                            Where.EQ("ObjectID",ID),
                            Where.EQ("PropertyID", PropertyID.ID),
                            Where.OrderBy("Created",Condition.Descendant)))
                        {
                            r = op;
                            break;
                        }
                    }

                    if (r == null)
                        r = PropInstance(ID, PropertyID, null);
                    //else IsNewObject = false;
                    return r;
                }));
            }
            finally
            {
                if (dbrequest) ClearChanges(PropertyID);
            }
        }

        /// <summary>
        /// Устанавливает последнее значение свойства объекта
        /// </summary>
        /// <param name="PropertyID">Тип устанавливаемого свойства</param>
        /// <param name="value">Новое значение</param>
        /// <returns>Признак того поменялось ли значение свойства объекта</returns>
        public override bool SetPropertyLastValue(PropType PropertyID, object value)
        {
            ObjectProp r = (ObjectProp)GetPropertyWithLastValue(PropertyID).Clone();
            r.Value = value;
             if (SetValue<ObjectProp>(PropertyID, r)) 
            {
                r.ID = Guid.NewGuid();
                r.Created = DateTime.Now;
                return true;
            };
            return false;
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

        protected override void PropertyChanged(PropType property, object Value)
        {
            base.PropertyChanged(property, Value);
            if (property == IDObject.Property.ID)
            {
                ArrayList l = new ArrayList();
                foreach (PropType pt in ChangedProperties)
                {
                    Object o = null;
                    if (ValueExists<T>(pt, out o))
                    {
                        l.Add(o);
                        DeleteProperty(pt);
                    }
                }
                
                foreach (ObjectProp op in l)
                    SetPropertyLastValue(op.Property, op.Value);
                l.Clear();
            }
        }


        public static PropertyIterator Query(RecordManager Manager, DynamicObject obj)
        {
            RecordIterator ri = RecordIterator.Enum<T>(Manager,
                Where.EQ("ObjectID", obj.ID),
                Where.OrderBy("Created",Condition.Descendant));

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
                Where.EQ("PropertyID",PropertyID.ID),
                Where.OP("ValueText",cmpop,v),
                Where.OrderBy("Created",Condition.Descendant));
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