using System;
using System.Collections;
using System.Collections.Generic;
using Softlynx.ActiveSQL;
using NUnit.Framework;
using System.Reflection;

namespace NUnit_tests
{
    namespace Models
    {
        public class PropSetObjA : PropertySet
        {
            public static class Property
            {
                static public PropType Prop1 = new PropType<string>();
                static public PropType Prop2 = new PropType<string>();
                static public PropType SubProp1 = new PropType<PropSetObjA>();
            }

            public string Prop1
            {
                get { return GetValue<string>(Property.Prop1, (string)null); }
                set { SetValue<string>(Property.Prop1, value); }
            }

            public string Prop2
            {
                get { return GetValue<string>(Property.Prop2, string.Empty); }
                set { SetValue<string>(Property.Prop2, value); }
            }

            public PropSetObjA SubProp1
            {
                get
                {
                    return GetValue<PropSetObjA>(Property.SubProp1, new DefaultValueDelegate<PropSetObjA>(delegate 
                    {
                        return new PropSetObjA();
                    })

                  );
                }
                set { SetValue<PropSetObjA>(Property.SubProp1, value); }
            }

        }


    }

    namespace FrameWorkClasses
    {

        [TestFixture()]
        public class Helpers
        {


            [Test(Description = "Check PropertySet simple property changes logic")]
            public void T00_PropertySetSimpleChanges()
            {
                Models.PropSetObjA psa = new NUnit_tests.Models.PropSetObjA();
                Assert.False(psa.HasChanges);
                psa.Prop1 = null;
                Assert.Contains(Models.PropSetObjA.Property.Prop1, psa.ChangedProperties);
                psa.ClearChanges();
                Assert.False(psa.HasChanges);
                string s = psa.Prop1;
                Assert.False(psa.HasChanges);
                s = psa.Prop2;
                Assert.False(psa.HasChanges);
                psa.Prop2 = string.Empty;
                Assert.False(psa.HasChanges);
                psa.Prop2 = "1";
                Assert.Contains(Models.PropSetObjA.Property.Prop2,psa.ChangedProperties);
                psa.SubProp1.Prop2 = "2";
                Assert.Contains(Models.PropSetObjA.Property.SubProp1, psa.ChangedProperties);
            }

            [Test(Description = "Check PropertySet sub properties changes tracing")]
            public void T10_PropertySetSimpleChanges()
            {
                Models.PropSetObjA psa = new NUnit_tests.Models.PropSetObjA();
                Assert.False(psa.HasChanges);
                Models.PropSetObjA psb =  psa.SubProp1;
                psb.Prop2 = "2";
                Assert.AreEqual(psa.ChangedProperties.Length, 1);
                Assert.Contains(Models.PropSetObjA.Property.SubProp1, psa.ChangedProperties);
            }


        }
    }
}
