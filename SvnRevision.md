# Introduction #

To handle AssemblyInfo.cs you should define Pre-Build event like
```
svnrevision $(ProjectDir)
svnrevision $(ProjectDir) "../$(ProjectName) CF/Properties/AssemblyInfo.cs"
```

This will change the files  `$(ProjectDir)/Properties/AssemblyInfo.cs` and `$(ProjectDir)/../$(ProjectName) CF/Properties/AssemblyInfo.cs` replacing **AssemblyVersion** and **AssemblyFileVersion** with evaluated values.

# Details #

The version is evaluated with help of svnversion based on following rules:
  * run `svnversion` with `-c` option to determine commited revision. If it has no any suffixes (pure number) than this will be result value.
  * run `svnversion` to determine current revision. Removes suffixes keeping pure number and increment it by 1 passing it as a result value.