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
    public delegate object DataProviderDelegate();

    public class CacheableObject
    {

        public static TimeSpan DefautlSlideTimeout = TimeSpan.FromMinutes(1);
        public static TimeSpan DefautlAbsoluteTimeout = TimeSpan.MaxValue;

        public TimeSpan SlideTimeout = DefautlSlideTimeout;
        public TimeSpan AbsoluteTimeout = DefautlAbsoluteTimeout;
        internal object key  = null;

        internal DateTime LastAccess = DateTime.Now;
        internal DateTime Created = DateTime.Now;
        private Object data = null;
        internal DataProviderDelegate provider = null;

        public object Key
        {
            get { return key; }
        }

        public Object Data
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

        public bool IsTimedOut(DateTime timepoint)
        {
            return ((timepoint - LastAccess > SlideTimeout) ||
                    (timepoint - Created > AbsoluteTimeout));
        }

        public bool IsTimedOut()
        {
            return IsTimedOut(DateTime.Now);
        }
    }

    public class CacheCollector:IDisposable
    {
        static Hashtable allcaches = new Hashtable();

        private Hashtable heap = new Hashtable();

        public void Clear()
        {
            heap.Clear();
        }

        public void Purge()
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
                heap.Remove(co.key);
            }
            candidates.Clear();
        }

        public CacheCollector()
        {
            lock (allcaches)
            {
                allcaches[this]=this;
            }
        }

        public void Dispose()
        {
            lock (allcaches)
            {
                allcaches.Remove(this);
            }
        }
        
        public static void ClearAll()
        {
            lock (allcaches)
            {
                foreach (CacheCollector cc in allcaches.Values)
                {
                    cc.Clear();
                }
            }
        }

        public static void PurgeAll()
        {
            lock (allcaches)
            {
                foreach (CacheCollector cc in allcaches.Values)
                {
                    cc.Purge();
                }
            }
        }

        public object Provide(object key, DataProviderDelegate provider)
        {
            CacheableObject co = (CacheableObject)heap[key];
            if (co == null)
            {
                co = new CacheableObject();
                co.key = key;
                co.provider = provider;
                heap[key] = co;
            }
            return co.Data;
        }

        public void Forget(object key)
        {
            heap.Remove(key);
        }

        public bool Exists(object key)
        {
            return heap.ContainsKey(key);
        }

    
    }
}
