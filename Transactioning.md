Just embrace the code with code like
```
        using (ManagerTransaction trans = RecordManager.Default.BeginTransaction())
        {
         ....
         trans.Commit();
        }
```

and it's automatically create and handle the transactions.