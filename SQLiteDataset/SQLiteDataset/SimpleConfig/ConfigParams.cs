using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;
using System.IO;


namespace Softlynx.SQLiteDataset.SimpleConfig
{
    [Serializable]
    [XmlRoot("SimpleConfig")]
    public class SerializableHashtable: Dictionary<String,String>, IXmlSerializable
    {
        
        XmlSchema IXmlSerializable.GetSchema()
        {
            return null;
        }
 
        void IXmlSerializable.ReadXml(XmlReader reader)
        { 
            reader.Read();
            while (reader.Name == "parameter")
            {
                string key=reader.GetAttribute("name");
                string value =reader.GetAttribute("value");
                this[key] = value;
                reader.Read();
            }
        }

        void IXmlSerializable.WriteXml(XmlWriter writer)
        {
            foreach (string key in Keys)
            {

                string value = this[key];
                writer.WriteStartElement("parameter");
                writer.WriteAttributeString("name", key.ToString());
                writer.WriteAttributeString("value", value.ToString());
                writer.WriteEndElement();
            }
        }
    }

    public static class SimpleConfig
    {
        internal static XmlSerializer formatter = null;
        public static SerializableHashtable Pairs = new SerializableHashtable();
        public static string FileName=string.Empty;

        static SimpleConfig()
        {
            formatter = new XmlSerializer(typeof(SerializableHashtable));
        }
        public static void Save()
        {
            using (FileStream strm = new FileStream(FileName,FileMode.Create))
            {
                formatter.Serialize(strm, Pairs);
                strm.Close();
            }
        }

        public static void Load()
        {
            using (FileStream strm = new FileStream(FileName, FileMode.Open))
            {
                Pairs=(SerializableHashtable)formatter.Deserialize(strm);
                strm.Close();
            }
        }

    }
}
