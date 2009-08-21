using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using System.Collections.Generic;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using System.Reflection;
using System.IO;

namespace Softlynx.ActiveSQL
{

    public abstract class PropType
    {
        internal static Hashtable propreg = new Hashtable();
        private Guid _id = Guid.Empty;
        private string _name = string.Empty;
        private Type _type = null;
        private bool anonymous = false;
        internal static Hashtable _PropsDependencies = new Hashtable();
        internal static Hashtable _PropsStack = new Hashtable();
        
        public bool Anonymous
        {
            get { return anonymous; }
        }

        internal static Hashtable ClassDependencyRoot(object o)
        {
            Hashtable ht=(Hashtable)_PropsDependencies[o.GetType()];
            if (ht==null) {
                ht=new Hashtable();
                _PropsDependencies[o.GetType()]=ht;
            }
            return ht;
        }

        internal static LinkedList<PropType> ClassStackRoot(object o)
        {
            LinkedList<PropType> hs = (LinkedList<PropType>)_PropsStack[o.GetType()];
            if (hs == null)
            {
                hs = new LinkedList<PropType>();
                _PropsStack[o.GetType()] = hs;
            }
            return hs;
        }

        internal PropType(Type type, string Name, string id)
        {
            if (id == null)
            {
                anonymous = true;
                ID = Guid.NewGuid();
            } else 
                ID = new Guid(id);

            PropType prevreg = (PropType)propreg[ID];
            if (prevreg != null)
                throw new Exception(string.Format("PropType {0}:{1} already registered under {2}:{3}",
                    Name, ID, prevreg.Name, prevreg.ID));
            propreg[ID] = this;
            _name = Name??string.Empty;
            _type=type;
           

        }

        override public string ToString()
        {
            return string.Format("{0}",(Anonymous?"(ANONYMOUS) ":string.Empty)+Name+":"+ID.ToString());
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

        public Type Type
        {
            get { return _type; }
        }


        public static string GetName(Guid PropertyID)
        {
            return (propreg[PropertyID] as PropType).Name;
        }

        public static PropType ByID(Guid PropertyID)
        {
            return (propreg[PropertyID] as PropType);
        }


    }
    
    public class PropType<T> : PropType
    {
        /// <summary>
        /// Резистрирует постоянное свойство с заданным ID.
        /// Применимо как для работы с DynamicObject, так и для 
        /// PropertySet.
        /// </summary>
        /// <param name="Name">Имя свойства</param>
        /// <param name="id">Его постоянный идентификатор</param>
        public PropType(string Name, string id):base(typeof(T),Name,id) {}

        /// <summary>
        /// Резистрирует анонимное свойство, используемое только в runtime для 
        /// работы с PropertySet.
        /// Недопускается использование анонимных свойств для работы с БД через 
        /// DynamicObject.
        /// </summary>
        public PropType() : base(typeof(T), null, null) { }

        /// <summary>
        /// Резистрирует анонимное свойство, используемое только в runtime для 
        /// работы с PropertySet.
        /// Недопускается использование анонимных свойств для работы с БД через 
        /// DynamicObject
        /// </summary>
        public PropType(string Name) : base(typeof(T), Name, null) { }
    }

    public static class ValueFormatter
    {
        internal static Hashtable serializers = new Hashtable();

        static private XmlSerializer GetSerializer(Type t)
        {
            XmlSerializer xr = (XmlSerializer)serializers[t];
            if (xr == null)
            {
                xr = new XmlSerializer(t, new XmlRootAttribute("VALUE"));
                serializers[t] = xr;
                serializers[t.Name] = xr;
            }
            return xr;
        }

        public static string Serialize(object o)
        {
            if (o == null) return null;
            XmlSerializer xs = GetSerializer(o.GetType());
            MemoryStream ms = new MemoryStream();
            xs.Serialize(ms, o);
            MemoryStream ms1 = new MemoryStream(ms.ToArray());
            XmlReader r = XmlReader.Create(ms1);
            return r.ReadElementString();
        }

        public static object Deserialize(Type t, string v)
        {
            if (v == null) return null;
            XmlSerializer xs = GetSerializer(t);
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms);
            xw.WriteElementString("VALUE", v);
            xw.Close();
            MemoryStream ms1 = new MemoryStream(ms.ToArray());
            object ov = xs.Deserialize(ms1);
            return ov;
        }

        static UTF8Encoding enc = new UTF8Encoding();

        public static string XmlStrFromBuffer(byte[] buf, int offset)
        {
            return enc.GetString(buf, offset, buf.Length - offset);
        }

        public static string XmlStrFromBuffer(byte[] buf)
        {
            return XmlStrFromBuffer(buf, 0);
        }


    }

    public static class ValueFormatter<T>
    {
        public static string Serialize(T o)
        {
            return ValueFormatter.Serialize(o);
        }

        public static T Deserialize(string v)
        {
            return (T)ValueFormatter.Deserialize(typeof(T),v);
        }

    }

    /// <summary>
    /// Модель хранит в себе один элемени из истории изменения свойсва объекта
    /// Изменение привязано к объекту ObjectID.
    /// Меняемое свойство имеет уникальный идентификатор PropertyID
    /// Время измнения храниться в Created
    /// Автор изменения AuthorID
    /// </summary>

    public abstract class ObjectProp:IDObject
    {
        public static class Props
        {
            public static PropType ObjectID = new PropType<Guid>("Object ID");
            public static PropType PropertyID = new PropType<Guid>("Property ID");
            public static PropType Created = new PropType<DateTime>("Created timestamp");
            public static PropType Value = new PropType<object>("Value");
            public static PropType ValueText = new PropType<string>("Serialized Value");
        }

        /// <summary>
        /// 
        /// </summary>
        public ObjectProp()
        {
        }

        /// <summary>
        /// Привязка к изменяемому объекту
        /// </summary>
        [Indexed]
        public Guid ObjectID
        {
            get { return GetValue<Guid>(Props.ObjectID, Guid.Empty); }
            set { SetValue(Props.ObjectID, value); }
        }

        /// <summary>
        /// Привязка к типу(имени) свойства меняемого объекта
        /// </summary>
        [Indexed]
        public Guid PropertyID
        {
            get { return GetValue<Guid>(Props.PropertyID, Guid.Empty); }
            set { SetValue(Props.PropertyID, value); }
        }

        public new PropType Property
        {
            get { return (PropType)PropType.propreg[PropertyID]; }
        }

        /// <summary>
        /// Когда это изменение произошло
        /// </summary>
        [Indexed]
        public DateTime Created
        {
            get { return GetValue<DateTime>(Props.Created, DateTime.Now); }
            set { SetValue(Props.Created, value); }
        }



        /// <summary>
        /// Новое значение
        /// </summary>
        [ExcludeFromTable]
        public Object Value
        {
            get
            {
                return GetValue<Object>(Props.Value, new DefaultValueDelegate<object>(
                    delegate
                    {
                        return ValueText == null 
                            ? null 
                            : ValueFormatter.Deserialize(PropType.ByID(PropertyID).Type, ValueText);
                    }));
            }
            set
            {
                if (value==null) {
                    DeleteProperty(Props.Value);
                    return;
                }
                if (SetValue(Props.Value, value))
                    SetValue(Props.ValueText,ValueFormatter.Serialize(value));
            }
        }

        protected override void PropertyChanged(PropType property, object Value)
        {
            base.PropertyChanged(property, Value);
            if (property == Props.ValueText)
                DeleteProperty(Props.Value);
        }

        [Indexed]
        public string ValueText
        {
            get { return GetValue<string>(Props.ValueText, (string)null); }
            set { SetValue(Props.ValueText, value); }
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

        public override bool Equals(object obj)
        {
            if (obj is ObjectProp)
            {
                if (Value==null) 
                 if ((obj as ObjectProp).Value==null) 
                     return true;
                     else
                     return false;
                return Value.Equals((obj as ObjectProp).Value);
            }
            else
                return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            if (Value==null) return 0;
            return Value.GetHashCode();
        }
    }

}