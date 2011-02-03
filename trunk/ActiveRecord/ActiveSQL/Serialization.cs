using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Xml;
using System.Globalization;
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
        static XmlWriterSettings _serializer_settings = null;

        public static XmlWriterSettings SerializerSettings
        {
            get
            {
                if (_serializer_settings == null)
                {
                    _serializer_settings = new XmlWriterSettings();
                    _serializer_settings.CloseOutput = false;
                    _serializer_settings.NewLineChars = "";
                    _serializer_settings.NewLineHandling = NewLineHandling.None;
                    _serializer_settings.NewLineOnAttributes = false;
                    _serializer_settings.OmitXmlDeclaration = true;
                    _serializer_settings.Indent = false;
                }
                return _serializer_settings;
            }
        }

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

        static CultureInfo _FP = null;
        static internal IFormatProvider SerializationFormat
        {
            get
            {
                if (_FP == null)
                {
                    _FP = new CultureInfo("");
                    _FP.NumberFormat.CurrencyDecimalSeparator = ".";
                    _FP.NumberFormat.NumberDecimalSeparator = ".";
                    _FP.NumberFormat.PercentDecimalSeparator= ".";
                    _FP.NumberFormat.CurrencyGroupSeparator = "";
                    _FP.NumberFormat.NumberGroupSeparator = "";
                    _FP.NumberFormat.PercentGroupSeparator = "";
                    _FP.DateTimeFormat.FullDateTimePattern = "yyyy-MM-ddTHH:mm:ss.FFFFFFFzzz";
                }
                return _FP;
            }
        }
        public static string Serialize(object o)
        {
            string r = string.Empty;

            if (o == null) return null;
            Type ot=o.GetType();

            
            if (ot == typeof(Int16))
                return XmlConvert.ToString((Int16)o);

            if (ot == typeof(Int32))
                return XmlConvert.ToString((Int32)o);
            
            if (ot == typeof(Int64)) 
                return XmlConvert.ToString((Int64)o);

            if (ot == typeof(string))
            return (string)o;

            if (ot == typeof(decimal))
                return XmlConvert.ToString((decimal)o);

            if (ot == typeof(double))
                return XmlConvert.ToString((double)o);

            if (ot == typeof(Single ))
                return XmlConvert.ToString((Single)o);

            if (ot == typeof(float))
                return XmlConvert.ToString((float)o);

            if (ot == typeof(Guid))
                return XmlConvert.ToString((Guid)o);

            if (ot == typeof(DateTime))
                return XmlConvert.ToString((DateTime)o, XmlDateTimeSerializationMode.Unspecified);

            if (ot == typeof(bool))
                return XmlConvert.ToString((bool)o);

            if (ot == typeof(char))
                return XmlConvert.ToString((int)(char)o);

            if (ot == typeof(byte))
              return XmlConvert.ToString((byte)o);

            if (ot.IsEnum)
                return o.ToString();

            if (ot == typeof(byte[]))
                return  Convert.ToBase64String((byte[])o);

            
            XmlSerializer xs = GetSerializer(ot);
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms, SerializerSettings);
                xs.Serialize(xw, o);
                xw.Close();
            ms.Position = 0;
            XmlReader xr = XmlReader.Create(ms);
                xr.Read();
                r = xr.ReadString();
                xr.Close();
            return r;
        }

        public static object Deserialize(Type t, string v)
        {
            if (v == null) return null;
            
            if (t == typeof(bool))
                return XmlConvert.ToBoolean(v);

            if (t == typeof(string))
                return v;

            if (t == typeof(Int16))
                return XmlConvert.ToInt16(v);

            if (t == typeof(Int32))
                return XmlConvert.ToInt32(v);

            if (t == typeof(Int64))
                return XmlConvert.ToInt64(v);

            if (t == typeof(decimal))
                return XmlConvert.ToDecimal(v);

            if (t == typeof(double))
                return XmlConvert.ToDouble(v);

            if (t == typeof(Single))
                return XmlConvert.ToSingle(v);

            if (t == typeof(char))
                return (char)XmlConvert.ToInt32(v);

            if (t == typeof(byte))
                return XmlConvert.ToByte(v);

            if (t == typeof(Guid))
                return XmlConvert.ToGuid(v);

            if (t == typeof(DateTime))
                return XmlConvert.ToDateTime(v,XmlDateTimeSerializationMode.Unspecified);

            if (t == typeof(byte[]))
            {
                byte[] res ={ };
                if (!v.Equals(string.Empty)) try
                    {
                        res=Convert.FromBase64String(v);
                    }
                    catch (FormatException) { };
                return res;
            }

            if (t.IsEnum)
                return Enum.Parse(t,v,true);
            
            XmlSerializer xs = GetSerializer(t);
            MemoryStream ms = new MemoryStream();
            XmlWriter xw = XmlWriter.Create(ms);
            xw.WriteElementString("VALUE", v);
            xw.Close();
            ms.Position = 0;
            object ov = xs.Deserialize(ms);
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

        //[Indexed]
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