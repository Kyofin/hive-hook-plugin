
# hive-hook-plugin


## Demo-Preview

安装本插件后，执行hive语句或者对HMS的api进行操作，会在`/tmp/hive`目录下产生日志。
![](http://image-picgo.test.upcdn.net/img/20210608144537.png)
字段血缘解析成Json。
```json
{
    "database": "default",
    "duration": 12852,
    "engine": "mr",
    "hash": "257a4ed67912909f67d562cb45ab7a2b",
    "jobIds": [
        "job_1623116014475_0007"
    ],
    "queryText": "insert into dw select id ,\"999\" from t",
    "timestamp": 1623134443,
    "user": "hive",
    "userGroupNames": [
        "staff",
        "everyone",
        "localaccounts",
        "_appserverusr",
        "admin",
        "_appserveradm",
        "_lpadmin",
        "access_bpf",
        "com.apple.sharepoint.group.2",
        "com.apple.sharepoint.group.3",
        "_appstore",
        "_lpoperator",
        "_developer",
        "_analyticsusers",
        "com.apple.access_ftp",
        "com.apple.access_screensharing",
        "com.apple.access_ssh-disabled",
        "com.apple.access_remote_ae",
        "com.apple.sharepoint.group.1"
    ],
    "version": "1.0",
    "columnLineages": [
        {
            "srcDatabase": "default",
            "destDatabase": "default",
            "edgeType": "PROJECTION",
            "sources": [
                {
                    "table": "t",
                    "column": "id"
                }
            ],
            "targets": [
                {
                    "table": "dw",
                    "column": "id"
                }
            ]
        },
        {
            "destDatabase": "default",
            "expression": "'999'",
            "edgeType": "PROJECTION",
            "sources": [],
            "targets": [
                {
                    "table": "dw",
                    "column": "name"
                }
            ]
        }
    ],
    "tableLineages": [
        {
            "srcDatabase": "default",
            "destDatabase": "default",
            "srcTable": "t",
            "destTable": "dw"
        }
    ]
}
```
## Table of contents
- [Demo-Preview](#Demo-Preview)
- [Usage](#Usage)
- [Installation](#Installation)
- [Development](#Development)

## Usage
[(Back to top)](#table-of-contents)
- 捕获HMS的操作
- 捕获对Hive表和库的操作
- 捕获读取数据的记录
- 捕获写出数据的记录
- 捕获字段级别数据血缘关系
- 形成日志文件

## Installation
[(Back to top)](#table-of-contents)
1. 将`hive-hook-plugin-1.0-SNAPSHOT.jar`复制到hive/lib目录内
2. 配置`hive-site.xml`文件
    ```shell script
    <property>
      <name>hive.metastore.event.listeners</name>
      <value>org.data.meta.hive.listener.MetaStoreListener</value>
    </property>
    <property>
      <name>hive.metastore.pre.event.listeners</name>
      <value>org.data.meta.hive.listener.MetaStorePreAuditListener</value>
    </property>
    <property>
      <name>hive.exec.post.hooks</name>
      <value>org.apache.hadoop.hive.ql.hooks.LineageLogger,org.data.meta.hive.hook.LineageLoggerHook</value>
    </property>
    ```
3. 打开hive cli 执行导数操作
```sql
insert into dw select id ,"999" from t;
```



# Development
[(Back to top)](#table-of-contents)

在项目根目录，执行mvn编译。
```shell script
mvn clean package
```
