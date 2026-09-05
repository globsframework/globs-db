# Globs SQL

Relational database access driven by a [GlobType](https://globsframework.org) instead of by an ORM mapping.
The `GlobType` *is* the table description — fields are columns, key fields are the primary key — and the
`Db*` annotations carry what SQL needs and the core metamodel does not express (table and column names, char
size, nullability, indexes, precision).

It works in both directions: `GlobType` → SQL (`createTable`, and the insert/update/delete/select builders)
and SQL → `GlobType` (`extractType(tableName)` / `extractFromQuery(sql)` build a type at runtime from JDBC
metadata).

The directory is `globs-db`; the artifact is **`globs-sql`**.

## Requirements

- Java 21
- `org.globsframework:globs` and `globs-gson` (composite fields are stored as JSON)
- **a JDBC driver of your own**: none is a dependency of this artifact. `JdbcSqlService` loads the driver
  class reflectively from the URL prefix, so the application ships the driver it needs.
- optionally `com.zaxxer:HikariCP` — see [Pooling](#pooling). It is an *optional* dependency, so it is not
  transitive: put it on your classpath and connections are pooled, leave it out and they are not.

Dialects with a driver here: HSQLDB, MySQL/MariaDB, PostgreSQL, Oracle, MS SQL Server.

## Installation

```xml
<dependency>
    <groupId>org.globsframework</groupId>
    <artifactId>globs-sql</artifactId>
    <version>5.2.0</version>
</dependency>
```

## The three layers

```
SqlService          per database  — JdbcSqlService(url, user, password) or DataSourceSqlService(pool)
  └── SqlConnection per transaction — sqlService.getDb() / getAutoCommitDb()
        └── builders  — getQueryBuilder / getCreateBuilder / getUpdateBuilder / getDeleteRequest
              └── SelectQuery (a prepared SELECT) | SqlRequest, BatchSqlRequest (a prepared DML)
```

`getDb()` is transactional (`commit()`, `rollback()`, `commitAndClose()`); `getAutoCommitDb()` gives a
connection where those are no-ops. A closed connection fails loudly rather than silently reconnecting.

## Transactions

`SqlConnection` is `AutoCloseable`. `close()` is idempotent, rolls back anything not committed, and never
throws — so it can sit in a `finally` without masking the exception that is unwinding the block:

```java
try (SqlConnection db = sqlService.getDb()) {
    db.getCreateBuilder(StudentType.TYPE).set(StudentType.name, "Ada").getRequest().apply();
    db.commit();
}   // rollback + release if commit was not reached
```

`SqlService` carries the same thing as a template, which is the recommended form — it makes leaking a
connection on an error path impossible:

```java
int id = sqlService.inTransaction(db -> {          // commit on return, rollback on any exception
    CreateBuilder create = db.getCreateBuilder(StudentType.TYPE);
    IntegerAccessor key = create.getKeyGeneratedAccessor(StudentType.id);
    create.set(StudentType.name, "Ada").getRequest().apply();
    return key.getInteger();
});

sqlService.runInTransaction(db -> db.getDeleteRequest(StudentType.TYPE, constraint).apply());

List<Glob> students = sqlService.read(db ->                 // auto-commit, for read-only work
        db.getQueryBuilder(StudentType.TYPE).selectAll().getQuery().executeAsGlobs());
```

The work may throw a checked exception; anything that is not already unchecked is wrapped in a
`SqlException` whose cause is the original.

## Failures, and retrying them

A driver exception is classified by its SQLState, refined by the vendor error code where a database lumps
distinct failures together — MySQL and Oracle report every integrity violation as 23000. The hierarchy is
what lets a caller decide what to do:

```
SqlException
├── ConstraintViolation          the data is at fault: a retry fails identically
│   ├── UniqueConstraintViolation      ├── NotNullViolation
│   └── ForeignKeyViolation            └── CheckConstraintViolation
├── TransientSqlException        concurrency or infrastructure: a retry may well work
│   ├── SerializationFailure           ├── LockTimeout
│   └── DeadlockDetected               └── ConnectionFailed
└── QueryCanceled                the statement was cancelled or timed out
```

`ConstraintViolation` is still the parent of the four specific ones, so an existing `catch` keeps working.
An unrecognised state stays a plain `SqlException` rather than being mislabelled.

That split is what makes a retry policy possible. It is off by default — replaying the lambda is only safe
when it does nothing outside the database, which this library cannot know:

```java
sqlService.setRetryPolicy(RetryPolicy.onTransientFailures(3, Duration.ofMillis(50)));

// or for one call site
sqlService.inTransaction(work, RetryPolicy.onTransientFailures(5, Duration.ofMillis(20)));
```

Only the `inTransaction` / `read` templates apply it, and only they can: they own the connection, so they
know the failed attempt was rolled back and that the next one starts clean. The delay doubles each time and
carries ±25% of jitter, so workers that collided once do not collide again in lockstep.

## Watching what runs

`SqlListener` is called after every statement, successful or not — the hook for a slow query log, a timing
histogram or a tracing span, without this artifact depending on any of them:

```java
sqlService.setListener(SqlListener.logSlowerThan(Duration.ofMillis(200)));

sqlService.setListener((sql, durationNanos, rowCount, error) ->
        metrics.timer("sql", "outcome", error == null ? "ok" : "error").record(durationNanos, NANOSECONDS));
```

`rowCount` is the number of rows a DML statement touched, or -1 when it is not known — a SELECT reports -1,
because at that point the result set has not been walked yet. The listener runs on the thread that executed
the statement, inside the transaction: keep it cheap, and let nothing escape from it.

## Pooling

`JdbcSqlService` pools its connections when HikariCP is on the classpath. Without it, and as before, each
`getDb()` opens a physical connection — the library logs which of the two it does at startup, and
`isPooled()` reports it.

```java
JdbcSqlService sqlService = new JdbcSqlService(url, user, password,
        PoolConfig.DEFAULT.withMaxPoolSize(20)
                          .withConnectionTimeout(Duration.ofSeconds(5))
                          .withPoolName("orders"));
...
sqlService.close();     // the service owns the pool
```

`PoolConfig.NO_POOL` opts out. `SqlService` is `AutoCloseable`: a service that borrows its `DataSource` from
its host (`DataSourceSqlService`) closes nothing, one that created its own pool closes it.

## Declaring a table

```java
GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("Student");
typeBuilder.addAnnotation(DbTableName.create("students"));
id        = typeBuilder.declareIntegerField("id", KeyField.ZERO, AutoIncrement.INSTANCE);
firstName = typeBuilder.declareStringField("firstName", DbMaxCharSize.create(255));
TYPE = typeBuilder.build();

sqlConnection.createTable(StudentType.TYPE);
```

The annotations, all with the usual Glob + `@interface` pair, live in `annotations/`: `DbTableName`,
`DbFieldName`, `DbMaxCharSize` / `DbMinCharSize`, `DbIsNullable`, `DbIndex` / `DbFieldIndex`,
`DbNumericPrecision` / `DbNumericDigit`, `DbSqlType`, `IsDbKey`, `IsTimestamp`, `IsBigDecimal`. `DbRef` is
declared and registered but nothing reads it — see [Indexes](#indexes).

### Indexes

`createTable` also creates the indexes the type declares, whether they come from the metamodel
(`addUniqueIndex` / `addNotUniqueIndex` on the builder, single or multi field) or from a `DbIndex`
annotation:

```java
typeBuilder.addNotUniqueIndex("byName", firstName);
sqlConnection.createTable(StudentType.TYPE);          // CREATE TABLE, then CREATE INDEX
```

An index name has to be unique per schema on PostgreSQL and HSQLDB, not per table, so the declared name is
qualified with the table: `byName` on `students` becomes `students_byName`. Only a table `createTable`
actually creates gets its indexes; on a table that already exists, call `createIndexes(type)` yourself,
because building an index there can be long and take locks. It skips the indexes the table already carries,
so it is safe to call twice.

**Foreign keys are not generated.** `DbRef` names a target type and nothing else — no column mapping — and
core keeps links in a separate `GlobLinkModel` rather than on the `GlobType`, so there is no source of truth
to generate a `REFERENCES` clause from. Generating them would also make `createTable` order-dependent and
would reject existing rows with dangling references. It needs a design decision, not a patch.

Date and time are annotation-driven, not type-driven: an `IntegerField` or `LongField` carrying `IsDate`,
`IsDateTime` (core) or `IsTimestamp` becomes a `DATE` / `DATETIME` / `TIMESTAMP` column.

`GlobField`, `GlobArrayField` and the union/array fields have no SQL equivalent — they are stored as **JSON
text** in a long-string column, which is why `globs-gson` is a compile dependency. Reading one back needs a
`GlobTypeResolver`.

## Inserting

```java
sqlConnection.getCreateBuilder(DummyWithDateTime.TYPE)
        .set(DummyWithDateTime.uuid, "AAAAA")
        .set(DummyWithDateTime.date, LocalDate.of(2022, 10, 3))
        .set(DummyWithDateTime.created, ZonedDateTime.of(LocalDate.of(2022, 10, 3),
                LocalTime.of(12, 0, 0), ZoneId.systemDefault()))
        .getRequest()
        .run();
sqlConnection.commit();
```

A generated key is read back through an accessor:

```java
CreateBuilder createBuilder = db.getCreateBuilder(StudentType.TYPE);
IntegerAccessor keyGenerated = createBuilder.getKeyGeneratedAccessor(StudentType.id);
try (SqlRequest insert = createBuilder.getRequest()) {
    insert.apply();
    int id = keyGenerated.getInteger();
}
```

`CreateBuilder` and `UpdateBuilder` take either a **value** or an **accessor** per field — an accessor turns
the request into a prepared statement fed from a stream, which is what `BatchSqlRequest` is for.

`populate(Collection<Glob>)` does that for you: it groups the globs by shape — the type together with the
columns actually written, which an unset auto-increment key makes vary inside one type — and batches each
group, flushing every 1000 rows. One prepared statement per shape, not per row.

## Querying

Nothing is materialized into objects unless you ask. Each selected field registers a `SqlAccessor` bound to
the `ResultSet`; `retrieve(field)` hands the typed accessor back, `select(field)` / `selectAll()` register it
silently for `executeAsGlobs()`.

```java
Ref<IntegerAccessor> idAccessor = new Ref<>();
Ref<StringAccessor> nameAccessor = new Ref<>();
SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.and(Constraints.equal(DummyObject.ID, 1),
                                Constraints.notEqual(DummyObject.NAME, "x")))
        .select(DummyObject.ID, idAccessor)
        .select(DummyObject.NAME, nameAccessor)
        .select(DummyObject.VALUE)
        .orderAsc(DummyObject.NAME)
        .top(10)
        .getQuery();

GlobStream requestStream = query.execute();
while (requestStream.next()) {
    int id = idAccessor.get().getValue(0);                                  // 0 if null
    String name = nameAccessor.get().getString();
    Object value = requestStream.getAccessor(DummyObject.VALUE).getObjectValue();
}
```

or, with the Globs built for you:

```java
List<Glob> all = sqlConnection.getQueryBuilder(StudentType.TYPE, Constraints.equal(StudentType.id, id))
        .selectAll()
        .getQuery()
        .executeAsGlobs();
```

`SelectQuery` also has `executeUnique()`, `executeOne()` (an `Optional`), `executeAsGlobStream()` and
`executeAsFieldValuesStream()`. `SelectBuilder` carries the aggregates (`count`, `sum`, `min`, `max`),
`groupBy`, `orderAsc` / `orderDesc`, `top`, `skip` and `withKeys`.

Two lifetimes to keep straight:

- **a builder is single-use** — `getQuery()` clears its accessors, so build one query per builder;
- **a query is multi-use** — `execute()` re-binds the constraint values on every call, so a query whose
  constraints were built from accessors is a prepared statement you re-fire with new parameters. That is the
  intended hot loop. `autoClose` is on by default (the statement closes when the stream is exhausted); use
  `getNotAutoCloseQuery()` and `close()` yourself to keep re-executing it.

`getQuery(String sql)` runs raw SQL, resolving the accessors' column indexes from `ResultSetMetaData`.

## Constraints

`Constraints` is a factory of immutable trees — `equal` / `notEqual`, `less` / `greater` and their
`strictly*` and `*Unchecked` variants, `in` / `notIn`, `isNull` / `isNotNull`, `contains`, `startWith`,
`regularExpressionCaseSensitive`, `and` / `or`, and the field-to-field `fieldEqual` — each overloaded per
field type so completion offers the right value type. Rendering and value binding are separate visitors, and `JSonConstraintTypeAdapter`
serializes a tree to and from JSON, so a constraint can cross a network boundary (a `FieldResolver` maps
`{type, name}` back to `Field`s).

## Upgrading a PostgreSQL database

**This is a breaking change for existing PostgreSQL databases.** `JdbcSqlService` used to give PostgreSQL
`DefaultNamingMapping`, which writes identifiers unquoted — and PostgreSQL folds an unquoted identifier to
lower case. A `GlobType` named `dummyObject` therefore became a table named `dummyobject`, and
`extractType("dummyObject")` could not find it again. `DataSourceSqlService`, going through
`MappingHelper`, did not agree: it already used `ToPostgreCaseNamingMapping`, which quotes a mixed-case
name so its case survives. The two now agree on the case-aware one.

New databases need nothing. For a database created by an earlier version, either rename the tables and
columns whose `GlobType` name is not all lower case:

```sql
ALTER TABLE dummyobject RENAME TO "dummyObject";
ALTER TABLE "dummyObject" RENAME COLUMN createdat TO "createdAt";
```

or keep the old behaviour by passing the mapping explicitly — an explicit `NamingMapping` is now honoured,
where it used to be silently dropped for every recognised dialect:

```java
new JdbcSqlService(url, user, password, DefaultNamingMapping.INSTANCE);
```

A `GlobType` whose name and fields are already all lower case is unaffected either way.

## Reading a schema back

```java
GlobType type = sqlConnection.extractType("students").extract();
GlobType fromQuery = sqlConnection.extractFromQuery("select a.id, b.name from a join b on ...");
```

Each generated field is annotated with what JDBC reported — `DbSqlType`, `DbFieldName`, `DbFieldIndex`,
`DbIsNullable`, `DbMaxCharSize` — so the extracted type can be serialized and reused as a declaration.

## Building

```bash
mvn -o test                                   # against an in-memory HSQLDB; no external database needed
mvn -o test -Dtest=SqlSelectQueryTest#testTop
```

`ERROR ... Unable to empty table` lines are expected noise on a green run — tests empty tables that do not
exist yet.

### Running against a real backend

The whole suite runs against a PostgreSQL started by Testcontainers, no editing required:

```bash
mvn test -Dglobs.test.db=postgresql               # -Dglobs.test.db.image=postgres:17 to pin another one
```

The container is started once per JVM and shared. When no container runtime is reachable the tests that
need one are skipped, not failed. `TestDb` holds the selection; HSQLDB stays the default, so a plain
`mvn test` still needs nothing installed. The driver and Testcontainers are `test`-scoped, so the published
artifact still ships no JDBC driver.

With Podman, point Testcontainers at its API socket:

```bash
podman system service --time=0 unix:///run/user/$(id -u)/podman/podman.sock &
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true
```

The suite is green on both backends.

`CLAUDE.md` documents what a new dialect involves and the traps around identifier escaping.

## License

Apache License 2.0 — see <https://www.apache.org/licenses/LICENSE-2.0.txt>.

## Links

- [Globs Framework](https://globsframework.org)
- [GitHub repository](https://github.com/globsframework/globs-db)
