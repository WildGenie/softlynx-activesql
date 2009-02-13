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

        internal PropType(Type type, string Name, string id)
        {
            ID = new Guid(id);
            PropType prevreg = (PropType)propreg[ID];
            if (prevreg != null)
                throw new Exception(string.Format("PropType {0}:{1} already registered under {2}:{3}",
                    Name, ID, prevreg.Name, prevreg.ID));
            propreg[ID] = this;
            _name = Name;
            _type=type;
           

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
        public PropType(string Name, string id):base(typeof(T),Name,id) {}
    }

    public static class ValueFormatter
    {
        internal static Hashtable serializers = new Hashtable();
        //internal static XmlWriterSettings serializers_settings = null;
        //internal static XmlSerializerNamespaces serializers_ns = null;


        /*
        static ValueFormatter()
        {

            serializers_settings = new XmlWriterSettings();
            serializers_settings.CloseOutput = true;
            serializers_settings.NewLineChars = "";
            serializers_settings.NewLineHandling = NewLineHandling.None;
            serializers_settings.NewLineOnAttributes = false;
            serializers_settings.OmitXmlDeclaration = true;
            serializers_settings.Indent = false;

            serializers_ns = new XmlSerializerNamespaces();
            serializers_ns.Add("", "");
        }
        */

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
            //return "";
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

    public abstract class ObjectProp
    {
        Guid _id = Guid.NewGuid();
        Guid _object_id = Guid.Empty;
        Guid _property_id = Guid.Empty;
        DateTime _created = DateTime.Now;
        Object _value = null;
        string _valuetext = null;

        /// <summary>
        /// 
        /// </summary>
        public ObjectProp()
        {
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
                _value = ValueFormatter.Deserialize(PropType.ByID(PropertyID).Type,_valuetext);
                return _value;
            }
            set
            {
                _value = value;
                _valuetext = ValueFormatter.Serialize(_value);
            }
        }

        [Indexed]
        public string ValueText
        {
            get { return _valuetext; }
            set { _valuetext = value; }
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

}