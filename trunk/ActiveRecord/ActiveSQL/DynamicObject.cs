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
    }
    abstract public class DynamicObject<T>: IIDObject,IComparable,IRecordManagerDriven,IDynamicObject
    {
        RecordManager _manager;
        Guid _id = Guid.Empty;
        bool _isnewobject = true;

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
        private Dictionary<PropType, ObjectProp> changed_values = new Dictionary<PropType, ObjectProp>();
        private Dictionary<PropType, ObjectProp> props_latest_value = new Dictionary<PropType, ObjectProp>();

        int IComparable.CompareTo(object other)
        {
            return ToString().CompareTo(other.ToString());
        }


        /// <summary>
        /// Возвращает признак ниличия изменеий в свойствах объекта
        /// </summary>
        public bool HasChanges
        {
            get { return changed_values.Values.Count > 0; }
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

        [AfterRecordManagerRead]
        protected void WhenRead()
        {
            IsNewObject = false;
        }

        /// <summary>
        /// Возвращает для объекта историю изменения его свойства PropertyID
        /// Если PropertyID=Guid.Empty то возвращается история изменения всех свойств.
        /// </summary>
        /// <param name="PropertyID">Тип свойства по которому следует ограничить список</param>
        /// <returns>Массив моделей T </returns>
        public RecordSet<T> GetPropertyChangeHistory(PropType PropertyID)
        {
            RecordSet<T> r = new RecordSet<T>();
            if (PropertyID == null)
            {
                r.Fill(
                    string.Format("{0}={1}",
                        Manager.AsFieldName("ObjectID"), Manager.AsFieldParam("ObjectID")),
                    string.Format("{0} DESC", Manager.AsFieldName("Created")),
                    "ObjectID", ID);
            }
            else
            {
                r.Fill(
                    string.Format("{0}={1} and {2}={3}",
                        Manager.AsFieldName("ObjectID"),Manager.AsFieldParam("ObjectID"),
                        Manager.AsFieldName("PropertyID"),Manager.AsFieldParam("PropertyID")),
                    string.Format("{0} DESC", Manager.AsFieldName("Created")),
                    "ObjectID",ID,
                    "PropertyID",PropertyID.ID);
            }
            return r;
        }

        /// <summary>
        /// Возвращается история изменения всех свойств.
        /// </summary>
        /// <returns>Массив моделей T</returns>
        public RecordSet<T> GetPropertyChangeHistory()
        {
            return GetPropertyChangeHistory(null);
        }

        /// <summary>
        /// Получить модель описывающую для объекта последнее значение свойства с типом 
        /// PropertyID
        /// </summary>
        /// <param name="PropertyID">Тип запрашиваемого свойства</param>
        /// <returns>Модель T из базы либо вновь созданная модель</returns>
        public ObjectProp GetPropertyWithLastValue(PropType PropertyID)
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
        public bool SetPropertyLastValue(PropType PropertyID, object value)
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

        /// <summary>
        /// Выбирает все записи об изменении свойства объекта с указанным сначением
        /// </summary>
        /// <param name="PropertyID">Искомое свойство</param>
        /// <param name="value">Искомое значение</param>
        /// <param name="ActualOnly">Указывает будет ли итоговый recordSet содержать все изменения или только те значения которые актуальны на текущий момент (последнее присвоенное)</param>
        /// <returns>RecordSet найденных записей по убыванию времени присвоения</returns>
        static public RecordSet<T> FindPropertiesWithValue(RecordManager Manager, PropType PropertyID, Object value, bool ActualOnly)
        {
            RecordSet<T> v = new RecordSet<T>(Manager);

            v.Fill(string.Format("{0}={1} and {2}={3}",
                        Manager.AsFieldName("PropertyID"), Manager.AsFieldParam("PropertyID"),
                        Manager.AsFieldName("ValueText"), Manager.AsFieldParam("ValueText")),
                   string.Format("{0} DESC", Manager.AsFieldName("Created")),
                   "PropertyID",PropertyID.ID,
                   "ValueText",ValueFormatter.Serialize(value));

            if (ActualOnly)
            {
                RecordSet<T> rv = new RecordSet<T>(Manager);
                Hashtable pf = new Hashtable();
                foreach (ObjectProp op in v)
                {
                    if (pf[op.ObjectID] == null)
                    {
                        rv.Add(op);
                        pf[op.ObjectID] = op;
                    }
                }
                v = rv;
            }
            return v;
        }

        /// <summary>
        /// Выбирает все записи об изменении свойства объекта с указанным сначением
        /// </summary>
        /// <param name="PropertyID">Искомое свойство</param>
        /// <param name="value">Искомое значение</param>
        /// <param name="ActualOnly">Указывает будет ли итоговый recordSet содержать все изменения или только те значения которые актуальны на текущий момент (последнее присвоенное)</param>
        /// <returns>RecordSet найденных записей по убыванию времени присвоения</returns>
          static public RecordSet<T> FindPropertiesWithValue(PropType PropertyID, Object value, bool ActualOnly)
        {
            return FindPropertiesWithValue(RecordManager.Default, PropertyID, value, ActualOnly);
        }


        /// <summary>
        /// Выбирает все записи c актуальным значением свойства объекта
        /// </summary>
        /// <param name="PropertyID">Искомое свойство</param>
        /// <param name="value">Искомое значение</param>
        /// <returns>RecordSet найденных записей по убыванию времени присвоения</returns>
        static public RecordSet<T> FindPropertiesWithValue(RecordManager Manager, PropType PropertyID, Object value)
        {
            return FindPropertiesWithValue(Manager,PropertyID, value, true);
        }

        static public RecordSet<T> FindPropertiesWithValue(PropType PropertyID, Object value)
        {
            return FindPropertiesWithValue(RecordManager.Default, PropertyID, value, true);
        }

        public static class Query<QT>
        {
            public static RecordSet<QT> FetchAll(RecordManager manager, PropType PropertyID, Object value, bool ActualOnly)
            {
                RecordSet<T> IdProps = DynamicObject<T>.FindPropertiesWithValue(manager,PropertyID, value, ActualOnly);
                RecordSet<QT> res = new RecordSet<QT>(manager);
                foreach (ObjectProp op in IdProps)
                {
                    Type TT = typeof(QT);
                    ConstructorInfo ci = typeof(QT).GetConstructor(new Type[0]);
                    DynamicObject<T> instance = ci.Invoke(null) as DynamicObject<T>;
                    instance.ID = op.ObjectID;
                    if (manager.Read(instance))
                    {
                        if (ActualOnly)
                        {
                            object v = instance.GetPropertyLastValue(PropertyID);
                            if ((v == null) || !v.Equals(value)) continue;
                        }
                        res.Add(instance);
                    }
                }
                return res;
            }

            public static RecordSet<QT> FetchAll(PropType PropertyID, Object value, bool ActualOnly)
            {
                return FetchAll(RecordManager.Default, PropertyID, value, ActualOnly);
            }


            public static RecordSet<QT> FetchAll(PropType PropertyID, Object value)
            {
                return FetchAll(PropertyID, value, true);
            }

            public static RecordSet<QT> FetchAll(RecordManager manager, PropType PropertyID, Object value)
            {
                return FetchAll(manager, PropertyID, value, true);
            }

        }
    }
}