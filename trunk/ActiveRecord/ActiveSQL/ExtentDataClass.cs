using System;
using System.Collections;
using System.Threading;
using System.Text;
using System.Data.Common;
using System.Collections.Generic;
using Softlynx.ActiveSQL;
using Softlynx.RecordSet;
using Softlynx.ActiveSQL.Replication;
using System.Reflection;

namespace Softlynx.ActiveSQL
{
    /// <summary>
    /// Base class to declare data class extentions
    /// </summary>
    public abstract class ExtentDataClass : IDObject
    {
        internal new class Property
        {
            static internal PropType BaseObjectID = new PropType<Guid>("Extendable object identifier");
        }

        /// <summary>
        /// Reference to parent object ID
        /// </summary>
        [Indexed]
        public Guid BaseObjectID
        {
            get { return GetValue<Guid>(Property.BaseObjectID, Guid.Empty); }
            set { SetValue<Guid>(Property.BaseObjectID, value); }
        }
    }

    /// <summary>
    /// Smart version of ExtentDataClass
    /// </summary>
    public abstract class ExtentDataSmartClass : ExtentDataClass, ISmartActiveRecord { };

    public partial class IDObject
    {
          
        /// <summary>
        /// Instantiate data extent with specified class and assign it to a specified property
        /// </summary>
        /// <typeparam name="T">Extent class type</typeparam>
        /// <param name="pt">Store property</param>
        /// <returns>New or existant instance populated from DB</returns>
        public T GetValue<T>(PropType pt) where T : ExtentDataClass
        {
            return GetValue<T>(pt,new DefaultValueDelegate<T> (
                delegate {

                    if (!HasID) 
                    {
                        throw new EIDObjectWithoutIDException("Can't extent object without ID");
                        // ID = Guid.NewGuid(); // ???
                    }
                    if (!pt.Type.Equals(typeof(T)))
                        throw new EPropTypeNotMatch("Property type "+pt.Type.Name+" does not match return type "+typeof(T).Name);

                    T res = null;
                    foreach (T co in RecordIterator.Enum<T>(RM, Where.Limit(1), Where.EQ("BaseObjectID", ID)))
                    {
                        res = co;
                        break;
                    }
                    if (res == null)
                    {
                        res = Activator.CreateInstance<T>();
                        (res as ExtentDataClass).ID = Guid.NewGuid();
                        (res as ExtentDataClass).BaseObjectID = ID;
                    }
                    return res;
                }));
        }

       
        private static Hashtable ClassExtentDataProps = new Hashtable();

        /// <summary>
        /// Delete all extents found for the class
        /// </summary>
        /// <param name="RM">RecordManager</param>
        protected virtual void DeleteReferencedExtentData(RecordManager RM)
        {
            List<PropType> ExtentDataProps = (List<PropType>)ClassExtentDataProps[this.GetType()];
            if (ExtentDataProps == null)
            {
                ExtentDataProps = new List<PropType>();
                foreach (PropType pt in PropertySet.Enum(this.GetType()).Values)
                {
                    if (typeof(ExtentDataClass).IsAssignableFrom(pt.Type))
                        if (RM.ActiveRecordInfo(pt.Type, false) != null)
                            ExtentDataProps.Add(pt);
                };
            ClassExtentDataProps[this.GetType()] = ExtentDataProps;
            }
            foreach (PropType pt in ExtentDataProps)
            {
                InTable table = RM.ActiveRecordInfo(pt.Type);
                using (DbDataReader reader = RM.CreateReader(
                    string.Format(
                    "select {0} from {1} where {2}={3}",
                    RM.AsFieldName("ID"),
                    RM.AsFieldName(table.Name),
                    RM.AsFieldName("BaseObjectID"),
                    RM.AsFieldParam("PRM0")), "PRM0", ID))
                {

                    object o = Activator.CreateInstance(pt.Type);
                    if (o is IRecordManagerDriven)
                        (o as IRecordManagerDriven).RecordManager = RM;
                    while (reader.Read())
                    {
                        if (reader.IsDBNull(0)) continue;
                        (o as IIDObject).ID = reader.GetGuid(0);
                        table.Delete(o,false);
                    }
                }
            }
        }
            
        
        /// <summary>
        /// Handle RecordManager deletions and if interface  ISmartActiveRecord defined
        /// call DeleteReferencedExtentData
        /// </summary>
        /// <param name="RM">Record Manager</param>
        [BeforeRecordManagerDelete]
        internal void RMDeleteReferencedExtentData(RecordManager RM)
        {
            if (this is ISmartActiveRecord)
                DeleteReferencedExtentData(RM);
        }

    }
}