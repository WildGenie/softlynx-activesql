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

    /// <summary>
    /// Exception generated when construct class does not match PropType.type 
    /// </summary>
    public class EPropTypeNotMatch : Exception
    {

        /// <summary>
        /// Construct EPropTypeNotMatch with message
        /// </summary>
        /// <param name="msg">Exception message</param>
        public EPropTypeNotMatch(string msg) : base(msg) { }

    };

    public delegate T DefaultValueDelegate<T>(PropType property);
    public delegate void PropertyValueChange(PropType property, object Value);

    /// <summary>
    /// Interface that able to report was it changed or not
    /// </summary>
    public interface ITrackChanges
    {
        /// <summary>
        /// Returns treu if obkect has changes able toreflect to layer down.
        /// </summary>
        bool HasChanges { get;}
    }

    /// <summary>
    /// ������ ��������� ������������ ������� �������.
    /// </summary>
    public abstract class PropertySet : ICloneable, ITrackChanges
    {

        /// <summary>
        /// turn on/off property dependancy tracking
        /// </summary>
        public static bool PerClassDependencyTracking = false;

        //Hashtable snapshot = new Hashtable();
        Hashtable changes = new Hashtable();
        Hashtable values = new Hashtable();


        /// <summary>
        /// Event fired up on property has been changed
        /// </summary>
        public event PropertyValueChange OnPropertyChanged = null;

        /// <summary>
        /// Event fired prior to property change
        /// </summary>
        public event PropertyValueChange OnPropertyChanging = null;

        /// <summary>
        /// ������ ����� �������� ��������
        /// </summary>
        /// <typeparam name="T">��� ��������</typeparam>
        /// <param name="property">������������� ��������</param>
        /// <param name="NewValue">����� ��������. </param>
        /// <returns>���� �� �������� ��������</returns>
        protected bool SetValue<T>(PropType property, T NewValue)
        {
            return SetValue<T>(property, NewValue, false);
        }
    


        private bool SetValue<T>(PropType property, T NewValue,bool IsDefault)
        {
            if ((!Nullable.Equals(NewValue, values[property.ID])) || (!values.ContainsKey(property.ID)))
            {
                if (!IsDefault)  PropertyChanging(property, NewValue);
                values[property.ID] = NewValue;
                if (NewValue is PropertySet)
                {
                    PropertySet ps = NewValue as PropertySet;
                    ps.OnPropertyChanged += new PropertyValueChange(delegate { changes[property.ID] = NewValue; });
                    if (ps.HasChanges && (!IsDefault)) changes[property.ID] = NewValue;
                }
                else
                {
                    if (!IsDefault) changes[property.ID] = NewValue;
                }
                if (!IsDefault) PropertyChanged(property, NewValue);
                return !IsDefault;
            }
            return false;
        }

        /// <summary>
        /// ����������� ����� ���������� �� ��������� �������� ��������
        /// </summary>
        /// <param name="property">������������� ��������</param>
        /// <param name="Value">��� ����� ��������</param>
        protected virtual void PropertyChanging(PropType property, object Value)
        {
            if (OnPropertyChanging != null)
                OnPropertyChanging(property, Value);
        }

        /// <summary>
        /// ����������� ����� ���������� � ������ ��������� �������� ��������
        /// </summary>
        /// <param name="property">������������� ��������</param>
        /// <param name="Value">��� ����� ��������</param>
        protected virtual void PropertyChanged(PropType property, object Value)
        {
            if (OnPropertyChanged != null)
                OnPropertyChanged(property, Value);
        }

        /// <summary>
        /// ������������� ������� �������� ��������, ���� ��� ��������� �� 
        /// ��� �������������� �������� DefaultValue � ��� �� ������������.
        /// </summary>
        /// <typeparam name="T">��� ����������</typeparam>
        /// <param name="property">������������� ��������</param>
        /// <param name="DefaultValue">�������� �� ���������</param>
        /// <returns>�������� �������������� ��������</returns>
        protected T GetValue<T>(PropType property, T DefaultValue)
        {
            return GetValue<T>(property, new DefaultValueDelegate<T>(delegate { return DefaultValue; }));
        }

        private LinkedList<PropType> PropStack
        {
            get
            {
                return PropType.ClassStackRoot(this);
            }
        }


        private Hashtable PropDependency
        {
            get
            {
                return PropType.ClassDependencyRoot(this);
            }
        }

        /// <summary>
        /// Returns the list of properties dependent on specified in prop param.
        /// Dependency is evaluated and tracked internaly. For instance if property 
        /// A in evaluation process requested value of property B then 
        /// A would be dependent on B and DependsOn(B) will return array with A.
        /// 
        /// For proper dependency tracking DO NOT use the prototype 
        /// GetValue&lt;T&gt;(PropType property,&lt;T&gt;DefaultValue) in your code, but use 
        /// the delegate patterns like 
        /// GetValue&lt;T&gt;(PropType property, DefaultValueDelegate&lt;T&gt;  DefaultValue)
        /// </summary>
        /// <param name="prop"></param>
        /// <returns></returns>
        public PropType[] PropsDependsOn(PropType prop)
        {
            Hashtable deps = (Hashtable)(PropDependency[prop] ?? new Hashtable());
            PropType[] res = new PropType[deps.Count];
            deps.Keys.CopyTo(res, 0);
            return res;
        }

        /// <summary>
        /// ������������� ������� �������� ��������, ���� ��� ��������� �� 
        /// ��� �������������� �������� ���������� ������ �������� DefaultValueDelegate
        /// � ��� �� ������������.
        /// </summary>
        /// <typeparam name="T">��� ����������</typeparam>
        /// <param name="property">������������� ��������</param>
        /// <param name="DefaultValue">������ ��� ��������� �������� �� ���������</param>
        /// <returns>������� �������������� ��������</returns>
        protected T GetValue<T>(PropType property, DefaultValueDelegate<T> DefaultValue)
        {
            if (PerClassDependencyTracking)
            {
                foreach (PropType pts in PropStack)
                {
                    if (pts.Equals(property)) continue;
                    Hashtable deps = (Hashtable)(PropDependency[property] ?? new Hashtable());
                    deps[pts] = true;
                    PropDependency[property] = deps;
                }
            }
            LinkedListNode<PropType> node = null;
            try
            {
                object obj = values[property.ID];
                if (typeof(T).IsInstanceOfType(obj))
                    return (T)obj;
                if (PerClassDependencyTracking)
                    node = PropStack.AddLast(property);
                T res = DefaultValue(property);
                SetValue<T>(property, res,true);
                return res;
            }
            finally
            {
                if (node != null)
                    PropStack.Remove(node);
            }
        }
        /// <summary>
        /// ���������� ������� �������� ������� � ��������� ���������, ���� ���� �������� ���������� � ����� 
        /// ��������� ��� T
        /// </summary>
        /// <typeparam name="T">����������� ���</typeparam>
        /// <param name="property">������������� ��������</param>
        /// <param name="result">�������� �������� ���� ��� ����������</param>
        /// <returns>�������� ������� ��� null ���� �� ����������</returns>
        protected bool ValueExists<T>(PropType property, out object result)
        {
            result = values[property.ID];
            if (typeof(T).IsInstanceOfType(result))
                return true;
            result = null;
            return false;
        }

        /// <summary>
        /// ���������� ������� �������������� �������� � �������� �����
        /// </summary>
        /// <typeparam name="T">����������� ���</typeparam>
        /// <param name="property">������������� ��������</param>
        /// <returns>���� ���� ���������� � false � ��������� ������</returns>
        protected bool ValueExists<T>(PropType property)
        {
            object tmp = null;
            return ValueExists<T>(property, out tmp);
        }

        /// <summary>
        /// ������� ������� �������� �������� � ���� ��������� �������,
        /// �������� ������� ������� ��������� � ��� ����������
        /// </summary>
        /// <param name="property">������������� ��������</param>
        /// <returns>���� �� �������� ���������� �� ��������</returns>
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
        /// Delete all the properties tracked as dependant on one specified as root
        /// </summary>
        /// <param name="root">Root property the dependency is tracked for</param>
        /// <returns></returns>
        public void DeleteDependantProperties(PropType root)
        {
            foreach (PropType pt in PropsDependsOn(root))
                DeleteProperty(pt);
        }

        /// <summary>
        /// �������� �������� ��� �� ������� �����������
        /// </summary>
        /// <param name="property">������������� ��������</param>
        public void ClearChanges(PropType property)
        {
            changes.Remove(property.ID);
        }

        /// <summary>
        /// �������� ���� ������, ��� �� ������� �����������
        /// </summary>
        [AfterRecordManagerRead]
        [AfterRecordManagerWrite]
        public void ClearChanges()
        {
            changes.Clear();
        }


        /*
        /// <summary>
        /// ��������� ������� �������� ������� ��� ������������ ���������
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
        /// ���������� ������� ��������� ������� �������������� ��������� ����� 
        /// snapshot ��� ���� �������� ������, �� ������������ ��������������� 
        /// ��������� �������
        /// </summary>
        public bool HasChanges
        {
            get { return changes.Count > 0; }
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
            target.values = (Hashtable)values.Clone();
            //target.snapshot = (Hashtable)snapshot.Clone();
            target.changes = (Hashtable)changes.Clone();
        }

        public void CopyFrom(PropertySet source)
        {
            source.CopyTo(this);
        }

        public virtual object Clone()
        {
            PropertySet ps = (PropertySet)this.MemberwiseClone();
            ps.CopyFrom(this);
            return ps;
        }

        /// <summary>
        /// ����������� ���������� �������� �������� �� values
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
                    object sv = source.values[de.Key];
                    if (de.Value.GetType().IsArray)
                    {
                        if (
                            ValueFormatter.Serialize(sv) != ValueFormatter.Serialize(de.Value))
                            return false;
                        continue;
                    }

                    if (!de.Value.Equals(sv))
                        return false;
                }
                return true;
            }
            else
                return base.Equals(obj);
        }

        /// <summary>
        /// ���������� ��� ��� � ������ ���� �������� values
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

        /// <summary>
        /// Fill the dictionary with all properties found on specified class and it successors 
        /// </summary>
        /// <returns>Dictionary with property name as key and PropType as value</returns>
        public static IDictionary<string, PropType> Enum<T>()
        {
            return Enum(typeof(T));
        }

        /// <summary>
        /// Fill the dictionary with all properties found on specified class and it successors 
        /// </summary>
        /// <returns>Dictionary with property name as key and PropType as value</returns>
        public static IDictionary<string, PropType> Enum(Type basetype)
        {
            Dictionary<string, PropType> allprops = new Dictionary<string, PropType>();
            Stack<Type> types = new Stack<Type>();
            types.Push(basetype);
            while (types.Count > 0)
            {
                Type t = types.Pop();
                foreach (Type nt in t.GetNestedTypes(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy))
                    types.Push(nt);

                foreach (MemberInfo mi in t.GetMembers(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy))
                {
                    if (mi.MemberType == MemberTypes.Field)
                    {
                        FieldInfo fi = t.GetField(mi.Name, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy);
                        if (fi.FieldType == typeof(PropType))
                        {
                            PropType v = (PropType)fi.GetValue(null);
                            allprops.Add(fi.Name, v);
                        }
                    }
                }
            }
            return allprops;
        }

    }



}
