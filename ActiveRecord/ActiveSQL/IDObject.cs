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

    public interface IIDObject
    {
        Guid ID { get; set;}
        bool HasID { get; }
    }


    /// <summary>
    /// Define interface to avoid useless ActiveRecord writes and handle them only when changes found.
    /// In advance it track down to write all changes properties from PropertySet down to objects tree.
    /// </summary>
    public interface ISmartActiveRecord { }
    
    /// <summary>
    /// Exception generated when write or reference operation take place on object without ID defined
    /// </summary>
    public class EIDObjectWithoutIDException : Exception {
        
        /// <summary>
        /// Construct EIDObjectWithoutIDException with message
        /// </summary>
        /// <param name="msg">Exception message</param>
        public EIDObjectWithoutIDException(string msg):base(msg){}
    
    };

    public abstract partial class IDObject:PropertySet,IIDObject,IRecordManagerDriven
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
        /// Determines if an ID property was ever defined
        /// </summary>
        public bool HasID
        {
            get {return ValueExists<Guid>(Property.ID);}
        }

        /// <summary>
        /// Determines was the object reflected 
        /// back (ever readed before) from database or constructed from scratch.
        /// Actualy it is true in case of Property.ID has been changed or does not even defined.
        /// </summary>
        [ExcludeFromTable]
        public bool IsNewObject
        {
            get {return IsChanged(Property.ID) || !HasID;  }
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

        /// <summary>
        /// Iterate all over changed properties and try write them if reported to have the changes.
        /// </summary>
        /// <param name="RM">Current RecordManager</param>
        /// <param name="WriteHandled">return true if no futher writing steps required</param>
        [BeforeRecordManagerWrite]
        protected void SmartWrites(RecordManager RM, ref bool WriteHandled)
        {
            if (!HasID && HasChanges)
            {
                throw new EIDObjectWithoutIDException("Can't write object without ID");
                // ID = Guid.NewGuid(); // ???
            }

            if (this is ISmartActiveRecord)
            {
                bool HasRef = false;
                foreach (PropType pt in ChangedProperties)
                {
                    object v = null;
                    if (ValueExists<ITrackChanges>(pt, out v) && (v as ITrackChanges).HasChanges)
                    {
                        InTable table = RM.ActiveRecordInfo(v.GetType(), false);
                        if (table != null)
                            if (table.Write(v, false) > 0) HasRef = true;
                        ClearChanges(pt);
                    }
                }
                WriteHandled = !(HasChanges || (HasRef && IsNewObject));
            }
        }
        

    }

    public abstract class IDSmartObject : IDObject, ISmartActiveRecord { };

    public interface IRecordManagerDriven
    {
        RecordManager RecordManager
        {
            get;
            set; 
        }
    }


}