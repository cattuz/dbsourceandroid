Android SQLite implementation of [Dalwit](//github.com/devexed/dalwit). Connections are configured with the `AndroidSQLiteConnection` class.

With default accessor configuration:

```java
Connection connection = new AndroidSQLiteConnection("./test.db");
Database database = connection.write();
/* ... */
```

With custom accessor configuration:

```java
Connection connection = new AndroidSQLiteConnection("./test.db", new DefaultAndroidSQLiteAccessorFactory());
Database database = connection.write();
/* ... */
```