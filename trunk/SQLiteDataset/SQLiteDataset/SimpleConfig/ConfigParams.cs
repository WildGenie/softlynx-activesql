using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.IO;

namespace Softlynx.SimpleConfig

{        
    public static class SimpleConfig
    {
        public static string FileName=string.Empty;
        public static Dictionary<String,String> Pairs=new Dictionary<string,string>();


        public static void Save()
        {
            using (FileStream strm = new FileStream(FileName, FileMode.Create))
            {
                using (XmlWriter writer = new XmlTextWriter(strm, null))
                {
                    writer.WriteStartElement("SimpleConfig");
                    foreach (string key in Pairs.Keys)
                    {
                        string value = Pairs[key];
                        writer.WriteStartElement("parameter");
                        writer.WriteAttributeString("name", key);
                        writer.WriteAttributeString("value", value);
                        writer.WriteEndElement();
                    }
                    writer.WriteEndElement();
                    writer.Close();

                }
                strm.Close();
            }
        }

        public static void Load()
        {
            using (FileStream strm = new FileStream(FileName, FileMode.Open))
            {
                Pairs.Clear();
                using (XmlReader reader = new XmlTextReader(strm))
                {
                    reader.ReadStartElement("SimpleConfig");
                    while (reader.Name == "parameter")
                    {
                        string key = reader.GetAttribute("name");
                        string value = reader.GetAttribute("value");
                        Pairs[key] = value;
                        reader.Read();
                    }
                    reader.ReadEndElement();
                    reader.Close();
                }
                    
                strm.Close();
            }
        }

    }
}
