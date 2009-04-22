using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Common;
using System.Reflection;
using Softlynx.ActiveSQL;

namespace Softlynx.RecordCache
{
    /// <summary>
    /// Called each time CacheCollector cant find the data for supplied key.
    /// Should return the instance of any object.
    /// </summary>
    /// <returns></returns>
    public delegate object DataProviderDelegate();

    /// <summary>
    ///
    /// Called each time CacheCollector removes the object from it's cache
    /// Should return the instance of any object.
    /// </summary>
    /// <param name="instance">The removed object instance</param>
    public delegate void ObjectPurgedDelegate(object instance);

    public class CacheableObject
    {
        public static TimeSpan DefautlSlideTimeout = TimeSpan.FromMinutes(1);
        public static TimeSpan DefautlAbsoluteTimeout = TimeSpan.MaxValue;

        internal TimeSpan SlideTimeout = DefautlSlideTimeout;
        internal TimeSpan AbsoluteTimeout = DefautlAbsoluteTimeout;
        internal object key = null;

        internal DateTime LastAccess = DateTime.Now;
        internal DateTime Created = DateTime.Now;
        private Object data = null;
        internal DataProviderDelegate provider = null;

        internal object Key
        {
            get { return key; }
        }

        internal Object Data
        {
            get
            {
                DateTime now = DateTime.Now;
                if (IsTimedOut(now) || (data == null))
                {
                    data = provider();
                    Created = now;
                }
                LastAccess = now;
                return data;
            }
        }

        internal bool IsTimedOut(DateTime timepoint)
        {
            return ((timepoint - LastAccess > SlideTimeout) ||
                    (timepoint - Created > AbsoluteTimeout));
        }

        internal bool IsTimedOut()
        {
            return IsTimedOut(DateTime.Now);
        }
    }

    public class CacheCollector:IDisposable
    {
        public TimeSpan DefautlSlideTimeout = CacheableObject.DefautlSlideTimeout;
        public TimeSpan DefautlAbsoluteTimeout = CacheableObject.DefautlAbsoluteTimeout;

        static Hashtable allcaches = new Hashtable();

        private Hashtable heap = new Hashtable();
        public event ObjectPurgedDelegate OnObjectPurge = null;

        public CacheCollector(TimeSpan SlideTimeout, TimeSpan AbsoluteTimeout):this()
        {
            DefautlSlideTimeout = SlideTimeout;
            DefautlAbsoluteTimeout = AbsoluteTimeout;
        }

        public CacheCollector(TimeSpan SlideTimeout)
            : this()
        {
            DefautlSlideTimeout = SlideTimeout;
        }

        public void Clear()
        {
            lock (heap.SyncRoot)
            {
                heap.Clear();
            }
        }

        public void Purge()
        {
            lock (heap.SyncRoot)
            {
                DateTime now = DateTime.Now;
                List<CacheableObject> candidates = new List<CacheableObject>();
                foreach (CacheableObject co in heap.Values)
                {
                    if (co.IsTimedOut(now))
                        candidates.Add(co);
                }
                foreach (CacheableObject co in candidates)
                {
                    Forget(co.Key);
                }
                candidates.Clear();
            }
        }

        public CacheCollector():base()
        {
            lock (allcaches.SyncRoot)
            {
                allcaches[this]=this;
            }
        }

        public void Dispose()
        {
            lock (allcaches.SyncRoot)
            {
                allcaches.Remove(this);
            }
        }
        
        public static void ClearAll()
        {
            lock (allcaches.SyncRoot)
            {
                foreach (CacheCollector cc in new ArrayList(allcaches.Values))
                {
                    cc.Clear();
                }
            }
        }

        public static void PurgeAll()
        {
            lock (allcaches.SyncRoot)
            {
                foreach (CacheCollector cc in new ArrayList(allcaches.Values))
                {
                    cc.Purge();
                }
            }
        }

        public object Provide(object key, DataProviderDelegate provider, TimeSpan SlideTimeout, TimeSpan AbsoluteTimeout)
        {
            CacheableObject co = null;
            lock (heap.SyncRoot)
                co = (CacheableObject)heap[key];

            if (co == null)
            {
                co = new CacheableObject();
                co.AbsoluteTimeout = AbsoluteTimeout;
                co.SlideTimeout = SlideTimeout;
                co.key = key;
                co.provider = provider;
                lock (heap.SyncRoot)
                    heap[key] = co;
            }
            return co.Data;
        }

        public object Provide(object key, DataProviderDelegate provider, TimeSpan SlideTimeout)
        {
            return Provide(key, provider, SlideTimeout, DefautlAbsoluteTimeout);
        }

        public object Provide(object key, DataProviderDelegate provider)
        {
            return Provide(key, provider, DefautlSlideTimeout, DefautlAbsoluteTimeout);
        }


        public void Forget(object key)
        {
            lock (heap.SyncRoot)
            {
                object o = heap[key];
                if (o is CacheableObject)
                {
                    if (OnObjectPurge != null) 
                        OnObjectPurge((o as CacheableObject).Data);
                    heap.Remove(key);
                }
            }
        }

        public bool Exists(object key)
        {
            return heap.ContainsKey(key);
        }

    
    }
}
