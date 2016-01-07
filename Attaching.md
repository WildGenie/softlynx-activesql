SQLite backend
```
using Softlynx.ActiveSQL;
using Softlynx.ActiveSQL.SQLite;

...

  ProviderSpecifics prov = new SQLiteSpecifics();
  prov.ExtendConnectionString("Data Source", @"c:\tests.db3");
  prov.ExtendConnectionString("BinaryGUID","FALSE");

```

PostgreSQL backend
```
  ProviderSpecifics prov = new PgSqlSpecifics();
  prov.ExtendConnectionString("Database", "test");
  prov.ExtendConnectionString("Host", "localhost");
  prov.ExtendConnectionString("User Id", "test");
  prov.ExtendConnectionString("Password", "test");
```

OleDB backend (highly experimental)
```
  ProviderSpecifics prov = new OleDBSpecifics();
  prov.ExtendConnectionString("provider", "Microsoft.Jet.OLEDB.4.0");
  prov.ExtendConnectionString("data source", @"C:\test.mdb");
  prov.ExtendConnectionString("Jet OLEDB:Database Password", "password");
```


Common final code
```
prov.Connection.Open();
RecordManager.Default=new RecordManager(prov, typeof(Program).Assembly.GetTypes());
```