In the common case you iterate over the class instances with help of RecordIterator.

The following code will iterate all over the SimpleObject instances reflected to database
```
using Softlynx.RecordSet;

....

   foreach (SimpleObject obj in RecordIterator.Enum<SimpleObject>())
    {
     ....
    }
```


The following code will iterate all over the SimpleObject instances reflected to database with specified restrictions
```
using Softlynx.RecordSet;

....

   foreach (SimpleObject obj in RecordIterator.Enum<SimpleObject>(
                      Where.OrderBy("Name"),
                      Where.EQ("Name","Value1")
           ))
    {
     ....
    }
```