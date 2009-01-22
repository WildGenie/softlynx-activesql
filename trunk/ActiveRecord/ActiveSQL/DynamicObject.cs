﻿using System;
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

    public class PropType
    {
        internal static Hashtable propreg = new Hashtable();
        private Guid _id = Guid.Empty;
        private string _name = string.Empty;
        public PropType(string Name, string id)
        {
            ID = new Guid(id);
            PropType prevreg = (PropType)propreg[ID];
            if (prevreg != null)
                throw new Exception(string.Format("PropType {0}:{1} already registered under {2}:{3}",
                    Name, ID, prevreg.Name, prevreg.ID));
            propreg[ID] = this;
            _name = Name;
        }

        override public string ToString()
        {
            return string.Format("{0}", Name);
        }

        override public bool Equals(Object obj)
        {
            return ID.Equals(((PropType)obj).ID);
        }

        override public int GetHashCode()
        {
            return ID.GetHashCode();
        }

        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }

        public string Name
        {
            get { return _name; }
        }


        public static PropType VoidType = new PropType("Void property", Guid.Empty.ToString());


        public static string GetName(Guid PropertyID)
        {
            return (propreg[PropertyID] as PropType).Name;
        }


    }


    abstract public class BaseSerializer
    {
        abstract public byte[] ToArray(object o);
        abstract public object FromArray(byte[] a);
        abstract public Type ManagedType();
        abstract public int TypeValue();
    }

    internal class IntSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(int); }
        override public byte[] ToArray(object o) { return BitConverter.GetBytes((int)o); }
        override public object FromArray(byte[] a) { return BitConverter.ToInt32(a, 0); }
        override public int TypeValue() { return 1; }
    }

    internal class StringSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(String); }
        override public byte[] ToArray(object o) { return UTF8Encoding.UTF8.GetBytes((string)o); }
        override public object FromArray(byte[] a) { return UTF8Encoding.UTF8.GetString(a, 0, a.Length); }
        override public int TypeValue() { return 2; }
    }

    internal class DoubleSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(Double); }
        override public byte[] ToArray(object o) { return BitConverter.GetBytes((Double)o); }
        override public object FromArray(byte[] a) { return BitConverter.ToDouble(a, 0); }
        override public int TypeValue() { return 3; }
    }

    internal class GuidSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(Guid); }
        override public byte[] ToArray(object o) { return ((Guid)o).ToByteArray(); }
        override public object FromArray(byte[] a) { return new Guid(a); }
        override public int TypeValue() { return 4; }
    }

    internal class DecimalSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(Decimal); }
        override public byte[] ToArray(object o) { return BitConverter.GetBytes(Decimal.ToDouble((decimal)o)); }
        override public object FromArray(byte[] a) { return new Decimal(BitConverter.ToDouble(a, 0)); }
        override public int TypeValue() { return 5; }
    }

    internal class DateTimeSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(DateTime); }
        override public byte[] ToArray(object o) { return BitConverter.GetBytes(((DateTime)o).Ticks); }
        override public object FromArray(byte[] a) { return new DateTime(BitConverter.ToInt64(a, 0)); }
        override public int TypeValue() { return 6; }
    }

    internal class BooleanSerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(Boolean); }
        override public byte[] ToArray(object o) { return BitConverter.GetBytes((Boolean)o); }
        override public object FromArray(byte[] a) { return BitConverter.ToBoolean(a, 0); }
        override public int TypeValue() { return 7; }
    }

    internal class ByteArraySerializer : BaseSerializer
    {
        override public Type ManagedType() { return typeof(Byte[]); }
        override public byte[] ToArray(object o) { return (Byte[])o; }
        override public object FromArray(byte[] a) { return a; }
        override public int TypeValue() { return 8; }
    }

    public static class ValueFormatter
    {
        static internal Hashtable SupportedTypes = new Hashtable();
        static internal Hashtable TypeValues = new Hashtable();

        static ValueFormatter()
        {
            RegisterSerializer(new IntSerializer());
            RegisterSerializer(new StringSerializer());
            RegisterSerializer(new DoubleSerializer());
            RegisterSerializer(new GuidSerializer());
            RegisterSerializer(new DecimalSerializer());
            RegisterSerializer(new DateTimeSerializer());
            RegisterSerializer(new BooleanSerializer());
            RegisterSerializer(new ByteArraySerializer());
        }

        static public void RegisterSerializer(BaseSerializer bs)
        {
            int type_code = bs.ManagedType().GetHashCode();
            SupportedTypes[type_code] = bs;
            TypeValues[bs.TypeValue()] = bs;
        }



        internal static byte[] Serialize(object o)
        {
            if (o == null) return null;
            int type_code = o.GetType().GetHashCode();
            BaseSerializer s = (BaseSerializer)SupportedTypes[type_code];
            if (s == null)
                throw new Exception("Unknown serialization method for type " + o.GetType().Name);
            byte[] v = s.ToArray(o);
            byte[] a = new byte[v.Length + 4];
            byte[] ta = BitConverter.GetBytes(s.TypeValue());
            Array.Copy(ta, 0, a, 0, ta.Length);
            Array.Copy(v, 0, a, 4, v.Length);
            return a;
        }

        internal static object Deserialize(byte[] a)
        {
            if (a.Length == 0) return null;
            int type_code = BitConverter.ToInt32(a, 0);
            BaseSerializer s = (BaseSerializer)TypeValues[type_code];
            if (s == null) throw new Exception("Unknown deserialization method for type code" + type_code.ToString());
            byte[] v = new byte[a.Length - 4];
            Array.Copy(a, 4, v, 0, v.Length);
            return s.FromArray(v);
        }

    }

    /// <summary>
    /// Модель хранит в себе один элемени из истории изменения свойсва объекта
    /// Изменение привязано к объекту ObjectID.
    /// Меняемое свойство имеет уникальный идентификатор PropertyID
    /// Время измнения храниться в Created
    /// Автор изменения AuthorID
    /// </summary>

    public abstract class ObjectProp
    {
        Guid _id = Guid.NewGuid();
        Guid _object_id = Guid.Empty;
        Guid _property_id = Guid.Empty;
        DateTime _created = DateTime.Now;
        Object _value = null;
        byte[] _valuearray = null;

        /// <summary>
        /// 
        /// </summary>
        public ObjectProp()
        {
        }

        public ObjectProp(Guid objectID, PropType propertyID, object value)
            : this()
        {
            ObjectID = objectID;
            PropertyID = propertyID.ID;
            Value = value;
        }

        /// <summary>
        /// Уникальный ключ записи в нижлежащей БД
        /// </summary>
        [PrimaryKey]
        public Guid ID
        {
            get { return _id; }
            set { _id = value; }
        }

        /// <summary>
        /// Привязка к изменяемому объекту
        /// </summary>
        [Indexed]
        public Guid ObjectID
        {
            get { return _object_id; }
            set { _object_id = value; }
        }

        /// <summary>
        /// Привязка к типу(имени) свойства меняемого объекта
        /// </summary>
        [Indexed]
        public Guid PropertyID
        {
            get { return _property_id; }
            set { _property_id = value; }
        }

        public PropType Property
        {
            get { return (PropType)PropType.propreg[PropertyID]; }
        }

        /// <summary>
        /// Когда это изменение произошло
        /// </summary>
        [Indexed]
        public DateTime Created
        {
            get { return _created; }
            set { _created = value; }
        }



        /// <summary>
        /// Новое значение
        /// </summary>
        [ExcludeFromTable]
        public Object Value
        {
            get
            {
                if (_value != null) return _value;
                if (_valuearray == null) return null;
                _value = ValueFormatter.Deserialize(_valuearray);
                return _value;
            }
            set
            {
                _value = value;
                _valuearray = ValueFormatter.Serialize(_value);
            }
        }

        [Indexed]
        public byte[] ValueArray
        {
            get { return _valuearray; }
            set { _valuearray = value; }
        }

        [ExcludeFromTable]
        public string AsString
        {
            get { return (string)Value; }
            set { Value = value; }

        }

        [ExcludeFromTable]
        public Boolean AsBoolean
        {
            get { return (Value == null) ? false : (Boolean)Value; }
            set { Value = value; }

        }


        [ExcludeFromTable]
        public decimal AsDecimal
        {
            get { return (Value == null) ? decimal.Zero : (decimal)Value; }
            set { Value = value; }

        }

        [ExcludeFromTable]
        public int AsInt
        {
            get { return (int)(Value == null ? 0 : Value); }
            set { Value = value; }
        }

        [ExcludeFromTable]
        public double AsDouble
        {
            get
            {
                return (double)(Value == null ? 0.0 : Value);
            }
            set { Value = value; }
        }

        [ExcludeFromTable]
        public Guid AsGuid
        {
            get { return Value == null ? Guid.Empty : (Guid)Value; }
            set { Value = value; }
        }

        [ExcludeFromTable]
        public DateTime AsDateTime
        {
            get
            {
                return (DateTime)(Value == null ? DateTime.MinValue : Value);
            }
            set { Value = value; }


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
    abstract public class DynamicObject<T>: IIDObject,IComparable,IRecordManagerDriven
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
    if (Manager == null)
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
                RecordSet<T> v = new RecordSet<T>(Manager);
                if (ID != Guid.Empty)
                {
                    v.Fill(
                        string.Format("{0}={1} and {2}={3}",
                           Manager.AsFieldName("ObjectID"), Manager.AsFieldParam("ObjectID"),
                           Manager.AsFieldName("PropertyID"), Manager.AsFieldParam("PropertyID")),
                        string.Format("{0} DESC", Manager.AsFieldName("Created")),
                        1,
                        "ObjectID",ID,
                        "PropertyID",PropertyID.ID);
                }

                if (v.Count == 0)
                    r = PropInstance(ID, PropertyID, null);
                else
                {
                    IsNewObject = false;
                    r = (ObjectProp)v[0];
                }
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
                        Manager.AsFieldName("ValueArray"), Manager.AsFieldParam("ValueArray")),
                   string.Format("{0} DESC", Manager.AsFieldName("Created")),
                   "PropertyID",PropertyID.ID,
                   "ValueArray",ValueFormatter.Serialize(value));

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