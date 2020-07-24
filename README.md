# MoyskleyTech.LINQToSQL

This repo is an implementation for the LINQ to SQL pattern. It is separated in 4 projects :

|Project|Function|
|--|--|
|MoyskleyTech.LINQToSQL | The main project, act as the LINQ to SQL layer|
|MoyskleyTech.LINQToSQL.Mysql | Proxy around MYSQL connector|
|MoyskleyTech.LINQToSQL.SQLITE | Proxy around |
|MoyskleyTech.LINQToSQL.Data | Proxy around System.Data for SQLServer|

To use this project, you must set
```c#
MoyskleyTech.LINQToSQL.StaticProxy = new MoyskleyTech.LINQToSQL.<PROXY_NAME>.<PROXY_NAME>Proxy();
//Ex:
MoyskleyTech.LINQToSQL.StaticProxy = new MoyskleyTech.LINQToSQL.Mysql.MysqlProxy();
```
