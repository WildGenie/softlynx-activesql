Each object with **InTable** attribute is subject to be reflected with RecordManager.
Any public RW property is reflected to corresponding data table column.

Each object **MUST** have a primary key defined, it can't by complex one, i.e. only one property should be marked with **PrimaryKey** attribute.

```
using Softlynx.ActiveSQL;

....

    [InTable]
    public class SimpleObject
    {
        Guid _ID = Guid.Empty;
        
        [PrimaryKey]
        public Guid ID
        {
            get { return _ID; }
            set { _ID = value; }
        }

        string _Name = string.Empty;

        public string Name
        {
            get { return _Name; }
            set { _Name = value; }
        }

        string _Address = string.Empty;

        //Will not be reflected due to it's protection level
        protected string Address
        {
            get { return _Address; }
            set { _Address = value; }
        }

        string _Status = string.Empty;

        //Will not be reflected due to direct attribute specified
        [ExcludeFromTable]
        public string Status
        {
            get { return _Status; }
            set { _Status = value; }
        }
    }
```

It is recommended to define a Guid typed column as a primary key for an object. Following that suggestion there is predefined class ID object that contains ID column defined as well as some other benefits. Almost the same reflection could be achieved on top of IDObject with following recommended code

```
using Softlynx.ActiveSQL;

....

    [InTable]
    public class SimpleObject2:IDObject
    {
        public class Property
        {
            static public PropType Name = new PropType<string>("Person Name");
            static public PropType Address = new PropType<string>("Person Address");
            static public PropType Status = new PropType<string>("Person Status");
        }
    
        public string Name
        {
            get { return GetValue<string>(Property.Name, string.Empty); }
            set { SetValue<string>(Property.Name, value); }
        }

        //Will not be reflected due to it's protection level
        protected string Address
        {
            get { return GetValue<string>(Property.Address, string.Empty); }
            set { SetValue<string>(Property.Address, value); }
        }

        //Will not be reflected due to direct attribute specified
        [ExcludeFromTable]
        public string Status
        {
            get { return GetValue<string>(Property.Status, string.Empty); }
            set { SetValue<string>(Property.Status, value); }
        }
    }
```

To control the reflection of the instance itself use the RecordManager methods like
  * Read
  * Write
  * Delete

For example reflect an instance to database:
```
                SimpleObject o = new SimpleObject();
                o.ID = Guid.NewGuid();
                o.Name = "Person One";

                RecordManager.Default.Write(o);
```

Get the instance values back from database:
```
                SimpleObject o = new SimpleObject();
                o.ID = new Guid("{A191C04B-C975-4cac-BDDE-CB0D0CA47E4E}");
                
                RecordManager.Default.Read(o);
```

Delete the specified instance permanently
```
                SimpleObject o = new SimpleObject();
                o.ID = new Guid("{A191C04B-C975-4cac-BDDE-CB0D0CA47E4E}");
                
                RecordManager.Default.Delete(o);
```