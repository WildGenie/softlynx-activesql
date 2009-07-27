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


    public delegate T DefaultValueDelegate<T>(PropType property);
    public delegate void PropertyValueChanged(PropType property, object Value);

    /// <summary>
    /// Объект управляет динамическим набором свойств.
    /// </summary>
    public abstract class PropertySet:ICloneable
    {
        //Hashtable snapshot = new Hashtable();
        Hashtable changes = new Hashtable();
        Hashtable values = new Hashtable();

        public event PropertyValueChanged OnPropertyValueChanged = null;
        /// <summary>
        /// Задает новое значение свойства
        /// </summary>
        /// <typeparam name="T">Тип свойства</typeparam>
        /// <param name="property">Идентификатор свойства</param>
        /// <param name="NewValue">Новое значение. </param>
        /// <returns>Было ли значение изменено</returns>
        protected bool SetValue<T>(PropType property, T NewValue)
        {
            if (!Nullable.Equals(NewValue,values[property.ID]))
            {
                values[property.ID] = NewValue;
                changes[property.ID] = NewValue;
                PropertyChanged(property,NewValue);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Виртуальный метод вызывается в случае изменения значения свойства
        /// </summary>
        /// <param name="property">Идентификатор свойства</param>
        /// <param name="Value">Его новое значение</param>
        protected virtual void PropertyChanged(PropType property, object Value)
        {
            if (OnPropertyValueChanged != null)
                OnPropertyValueChanged(property, Value);
        }

        /// <summary>
        /// Запрашивается текущее значение свойства, если оно отсутвует то 
        /// ему присваеевается значение DefaultValue и оно же возвращается.
        /// </summary>
        /// <typeparam name="T">Тип результата</typeparam>
        /// <param name="property">Идентификатор свойства</param>
        /// <param name="DefaultValue">Значение по умолчанию</param>
        /// <returns>Значение запрашиваемого свойства</returns>
        protected T GetValue<T>(PropType property, T DefaultValue)
        {
            return GetValue<T>(property, new DefaultValueDelegate<T>(delegate { return DefaultValue; }));
        }

        /// <summary>
        /// Запрашивается текущее значение свойства, если оно отсутвует то 
        /// ему присваеевается значение результата работы делегата DefaultValueDelegate
        /// и оно же возвращается.
        /// </summary>
        /// <typeparam name="T">Тип результата</typeparam>
        /// <param name="property">Идентификатор свойства</param>
        /// <param name="DefaultValue">елегат для получения значения по умолчанию</param>
        /// <returns>начение запрашиваемого свойства</returns>
        protected T GetValue<T>(PropType property, DefaultValueDelegate<T> DefaultValue) 
        {
            object obj = values[property.ID];
            if (typeof(T).IsInstanceOfType(obj))
                return (T)obj;
            T res = DefaultValue(property);
            SetValue<T>(property, res);
            return res;
        }

        /// <summary>
        /// Возвращает текущее значение объекта с указанным свойством, если само значение определено и имеет 
        /// указанный тип T
        /// </summary>
        /// <typeparam name="T">Проверяемый тип</typeparam>
        /// <param name="property">Идентификатор свойства</param>
        /// <param name="result">Значение свойства если оно существует</param>
        /// <returns>Значение объекта или null если не существует</returns>
        protected bool ValueExists<T>(PropType property,out object result)
        {
            result= values[property.ID];
            if (typeof(T).IsInstanceOfType(result))
                return true;
            result = null;
            return false;
        }

        /// <summary>
        /// Удаляет текущее значение свойства и если изменения сделаны,
        /// вызывает цепочку событий связанное с его изменением
        /// </summary>
        /// <param name="property">Идентификатор свойства</param>
        /// <returns>Было ло свойство определено до удаления</returns>
        public bool DeleteProperty(PropType property)
        {
            if (values.Contains(property.ID))
            {
                values.Remove(property.ID);
                ClearChanges(property);
                PropertyChanged(property, null);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Помечает свойство как не имеющее модификаций
        /// </summary>
        /// <param name="property">Идентификатор свойства</param>
        public void ClearChanges(PropType property)
        {
            changes.Remove(property.ID);
        }

        /// <summary>
        /// Помечает весь объект, как не имеющий модификаций
        /// </summary>
        [AfterRecordManagerRead]
        [AfterRecordManagerWrite]
        public void ClearChanges()
        {
            changes.Clear();
        }


        /*
        /// <summary>
        /// Фиксирует текущие значения свойств для отслеживания изменений
        /// </summary>
        public void Snapshot()
        {
            //snapshot.Clear();
            changes.Clear();
            foreach (DictionaryEntry de in values)
            {
                snapshot[de.Key] = (de.Value is ICloneable) ? (de.Value as ICloneable).Clone() : de.Value;
            }
        }
        */

        /// <summary>
        /// Возвращает признак изменения свойств относительного сделаного ранее 
        /// snapshot или если снапшота небыло, то относительно первоначального 
        /// состояния объекта
        /// </summary>
        public bool HasChanges
        {
            get {return changes.Count>0;}
        }

        public PropType[] ChangedProperties
        {
            get 
            {
                PropType[] res = new PropType[changes.Count];
                int cnt = 0;
                foreach (Guid id in changes.Keys) 
                    res[cnt++] = PropType.ByID(id);
                return res;
            }
        }

        public bool IsChanged(PropType property)
        {
            return changes.ContainsKey(property.ID);
        }

        public void CopyTo(PropertySet target)
        {
            target.values=(Hashtable)values.Clone();
            //target.snapshot = (Hashtable)snapshot.Clone();
            target.changes = (Hashtable)changes.Clone();
        }

        public void CopyFrom(PropertySet source)
        {
            source.CopyTo(this);
        }

        public virtual object Clone()
        {
            PropertySet ps=(PropertySet)this.MemberwiseClone();
            ps.CopyFrom(this);
            return ps;
        }

        /// <summary>
        /// Поэлементно сравнивает значения объектов из values
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is PropertySet)
            {
                PropertySet source = obj as PropertySet;

                if (!source.GetType().IsAssignableFrom(this.GetType()))
                    return false;
                
                if (source.values.Count != values.Count) 
                    return false;

                foreach (DictionaryEntry de in values)
                {
                    if (!de.Value.Equals(source.values[de.Key])) 
                        return false;
                }
                return true;
            } else 
            return base.Equals(obj);
        }
        
        /// <summary>
        /// Возвращает хеш код с учетом всех значений values
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int hc = 0;
            foreach (object o in values.Values)
            {
                hc ^= o.GetHashCode();
            }
            return hc;
        }
    }

    public class IDObject:PropertySet,IIDObject,IRecordManagerDriven
    {

        public class Property
        {
            static public PropType ID = new PropType<Guid>("IDObject identifier");
        }

        [PrimaryKey]
        public Guid ID
        {
            get { return GetValue<Guid>(Property.ID,Guid.Empty); }
            set { SetValue<Guid>(Property.ID,value); }
        }
        /// <summary>
        /// Determines was the object reflected 
        /// back (ever readed before) from database or constructed from scratch.
        /// Actualy it is true in case of Property.ID has been changed or does not even defined.
        /// </summary>
        [ExcludeFromTable]
        public bool IsNewObject
        {
            get { 
                object tmp=null;
                return IsChanged(Property.ID) || (!ValueExists<Guid>(Property.ID,out tmp)); }
        }

        private RecordManager _RM = null;
        /// <summary>
        /// Associated record manager or null if not defined
        /// </summary>
        [ExcludeFromTable]
        public RecordManager RecordManager
        {
            get { return _RM; }
            set { _RM=value; }
        }

        /// <summary>
        /// Returns either associated record manager or RecordManager.Default.
        /// </summary>
        public RecordManager RM
        {
            get
            {
                if (_RM == null) _RM = RecordManager.Default;
                return _RM;
            }
        }
    }

    public interface IRecordManagerDriven
    {
        RecordManager RecordManager
        {
            get;
            set; 
        }
    }


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
    abstract public class DynamicObject : IDObject, IComparable, IDynamicObject
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
       

        [BeforeRecordManagerWrite]
        protected void ActiveRecordWrite(RecordManager manager,ref bool handled)
        {
            if (!handled)
            {
                foreach (PropType pt in  ChangedProperties)
                {
                    Object o = null;
                    if (ValueExists<T>(pt, out o))
                    {
                        manager.Write(o);
                        ClearChanges(pt);
                    }
                }
                handled=!HasChanges;
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

                    if (r == null) r = PropInstance(ID, PropertyID, null);
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