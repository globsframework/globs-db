# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This repo is one clone inside the `globs` workspace — read `../CLAUDE.md` first for the ecosystem-wide
conventions (Glob metamodel, annotations as Globs, no-reflection-on-the-hot-path, per-repo release cycles).

## What this repo is

Directory `globs-db` builds artifact **`org.globsframework:globs-sql`**: relational DB access driven by a
`GlobType` instead of by an ORM mapping. A `GlobType` *is* the table description (fields → columns, key
fields → primary key), and the `Db*` annotations on the type/fields carry everything SQL needs that the core
metamodel doesn't express (column name, table name, char size, nullability, index, precision).

It works in both directions:
- `GlobType` → SQL: `createTable`, insert/update/delete/select builders.
- SQL → `GlobType`: `SqlConnection.extractType(tableName)` / `extractFromQuery(sql)` build a `GlobType` at
  runtime from JDBC metadata (`DefaultGlobTypeExtractor`, `utils/ExtractType`), annotating each generated
  field with `DbSqlType`, `DbFieldName`, `DbFieldIndex`, `DbIsNullable`, `DbMaxCharSize`, …

## Build & test

Java 21 (CI builds on 17). Everything runs against an **in-memory HSQLDB** (`jdbc:hsqldb:.`), so the full
suite needs no external database:

```bash
mvn -o test                                   # ~54 tests, 5 skipped, offline once ~/.m2 is warm
mvn -o test -Dtest=SqlSelectQueryTest         # one class
mvn -o test -Dtest=SqlSelectQueryTest#testTop # one method
mvn -s settings.xml -B package                # what CI runs (needs GH_MAVEN_REGISTRY_USER/_ACCESS_TOKEN)
```

Test-suite facts worth knowing before chasing a "failure":
- ERROR lines like `Unable to empty table : DELETE FROM …` are expected noise — tests call `emptyTable` on
  tables that don't exist yet. A green run still prints them.
- The backend is chosen by `-Dglobs.test.db` and served by `TestDb` (`src/test/.../sql/testdb/`): `hsqldb`
  (default, in memory) or `postgresql`, started by Testcontainers and shared by every test class in the JVM.
  `mvn test -Dglobs.test.db=postgresql` runs the *whole* suite against a real PostgreSQL; with no container
  runtime reachable those tests skip rather than fail. `-Dglobs.test.db.image` pins another image.
- The suite is green on both backends. `NamingMappingTest` pins which mapping each url resolves to.
- `OracleTest` is still `@Ignore`d (reads `-Doracle.url/-Doracle.user/-Doracle.pwd`) and
  `SqlExceptionTest.testConcurrentModification` is `@Ignore`d because it asserts a dirty read no backend
  grants by default. `scripts/podman-postgres.sh` / `podman-oracle.sh` remain for a hand-driven database.
- **JDBC drivers are not runtime dependencies of this artifact** (commit c880a39 removed them deliberately);
  `JdbcSqlService` loads the driver class *reflectively by name* from the URL prefix, so consuming apps ship
  their own. The PostgreSQL driver and Testcontainers are `test`-scoped here, which is what makes the
  backend switch above work without editing `pom.xml`.
- Tests are **JUnit 4**; most extend `DbServicesTestCase`, model types live in `src/test/.../sql/model/`
  (`DummyObject`, `DummyObject2`, `DummyWithDateTime`, …).

## Architecture

### The three layers

```
SqlService          (per database: URL/credentials/DataSource + NamingMapping) — JdbcSqlService, DataSourceSqlService
  └── SqlConnection (per JDBC Connection, per transaction)                      — JdbcConnection + one subclass per dialect
        └── builders: SelectBuilder / CreateBuilder / UpdateBuilder / getDeleteRequest
              └── SelectQuery (a prepared SELECT) | SqlRequest / BatchSqlRequest (a prepared DML)
```

`sqlService.getDb()` gives a transactional connection (`commit()`, `rollback()`, `commitAndClose()`);
`getAutoCommitDb()` gives an auto-commit one where commit/rollback are no-ops. `JdbcConnection` nulls its
`Connection` field on close and every entry point calls `checkConnectionIsNotClosed()`, so a closed
connection fails loudly rather than silently reconnecting.

Both layers are `AutoCloseable`. `SqlConnection.close()` is idempotent, rolls back uncommitted work and
logs rather than throws (it is meant for a `finally`, where throwing would mask the real exception);
`commitAndClose()` stays the call to make when the outcome of the commit matters. `SqlService` has the
transaction templates as `default` methods — `inTransaction` / `runInTransaction` (commit on return,
rollback on exception, always released) and `read` / `runRead` on an auto-commit connection — so any
implementation gets them; prefer them in new code over a hand-rolled try/finally.

`JdbcSqlService` pools connections through `drivers/jdbc/pool/`. HikariCP is an **optional** Maven
dependency: `ConnectionPools.isAvailable()` checks for it and `HikariConnectionPool` is the only class that
references it, so it is only ever loaded once that check passed (the call is also guarded against
`LinkageError`). No pool on the classpath, or `PoolConfig.NO_POOL`, means one physical connection per
`getDb()` — the pre-pooling behaviour. The service owns its pool and must be `close()`d;
`DataSourceSqlService` borrows its `DataSource` and closes nothing. `JdbcSqlService` now resolves its
dialect through `DbType.fromString(url)`, so an unrecognised URL fails at construction instead of NPE-ing
on the first `getDb()`.

### Accessors: why `select` and `retrieve` both exist

Nothing here materializes rows into objects unless you ask for it. The builder registers a `SqlAccessor` per
selected field (`accessors/`), all of them are bound to the `SqlGlobStream` wrapping the `ResultSet`, and
each accessor reads column *N* of the current row on demand. Hence the two shapes on `SelectBuilder`:

- `retrieve(field)` returns the typed accessor; `select(field, Ref<...>)` is the same thing writing into a
  `Ref`; `select(field)` / `selectAll()` register the accessor without handing it back (use
  `executeAsGlobs()` / `executeAsGlobStream()` to get `Glob`s built from them).
- Symmetrically, `CreateBuilder`/`UpdateBuilder` accept either a **value** or an **accessor** per field.

Consequences that bite:
- **A builder is single-use**: `getQuery()` clears `fieldToAccessorHolder` in a `finally`. Build one query
  per builder.
- **A `SelectQuery` is multi-use**: `execute()` re-binds constraint values through `ValueConstraintVisitor`
  on every call, so a query whose constraints were built from accessors is a prepared statement you re-fire
  with new parameter values — that is the intended hot loop.
- `autoClose` defaults to true: the `PreparedStatement` is closed once the result stream is exhausted. Use
  `getNotAutoCloseQuery()` to keep re-executing, and `close()` yourself.
- `getQuery(String sql)` runs raw SQL and resolves accessor column indexes from `ResultSetMetaData` instead
  of from the generated SQL.

### Failure classification, retry, listener

`SqlExceptions.typed(sql, e)` is the single place that maps a `SQLException` onto this library's hierarchy:
SQLState first, vendor error code to refine it where a database lumps failures under one state (MySQL and
Oracle both use 23000 for every integrity violation; MySQL reports a deadlock as 40001 like a serialization
failure). The split that matters is `ConstraintViolation` (the data is at fault, a retry fails identically)
versus `TransientSqlException` (concurrency or infrastructure, a retry may work) — `RetryPolicy` keys on the
latter. Unknown states stay a plain `SqlException`; adding a state means one line in `SqlExceptions.kindOf`
plus a case in `SqlExceptionsTest`.

`JdbcConnection.getTypedException` delegates there and is still overridable per dialect. Watch for execution
sites that bypass it: `SqlUpdateRequest`, `SqlDeleteBuilder` and `SqlSelectQuery` used to throw
`UnexpectedApplicationState` or a bare `SqlException`, which hid constraint violations behind a type nobody
catches. They all go through `SqlExceptions` now.

Only the `SqlService` templates apply a `RetryPolicy`, because only they know the failed attempt was rolled
back. `SqlListener` is fetched from `sqlService` at each execution — no constructor plumbing — and notified
in both the success and the failure path of `SqlSelectQuery.execute`, `SqlCreateRequest.apply`/`applyBatch`,
`SqlUpdateRequest.apply` and `SqlDeleteBuilder.apply`. A new execution path should notify it too.

### Joins

`TableRef` is one *occurrence* of a table (a `GlobType` plus an alias), `ColumnRef` a column of one. That
distinction is the whole feature: a bare `Field` cannot say which side of a self join it means. The builder
hands them out (`rootTable()`, `table(type)`) and `innerJoin`/`leftJoin` add a `Join` to the spec.

The rendering has two modes and `SqlSelectQuery` picks between them on `joins.isEmpty()`:
`ColumnQualifier.byTableName` reproduces the old behaviour exactly — table names, FROM built from the
`globTypes` the rendering collects — while the alias qualifier writes `t0`, `t1` and builds FROM from the
root plus the joins. **A query without joins must keep generating byte-identical SQL**; `JoinTest` pins that.

Things that bite:
- a **glob has one type**, so a join's other side cannot be materialised into it (`AccessorGlobBuilder`
  refuses two types in one stream). Joined columns are read through accessors, which is why
  `retrieveUnTyped(ColumnRef)` goes through the `SqlOperation` list rather than `fieldToAccessorHolder`;
- **ON values bind before WHERE values**, since ON comes first in the statement — `execute()` runs a
  `ValueConstraintVisitor` per join and carries its `getIndex()` forward;
- **no `AS` before a table alias**: Oracle accepts `AS` only in front of a column alias;
- the alias resolution refuses a bare field whose type appears twice, and one whose type is not joined at
  all — mixing explicit joins with the old implicit cross join would otherwise produce a broken FROM.

Adding an alias-aware constraint means giving the constraint class an optional `TableRef` and a factory
overload in `Constraints`; the visitor signatures took `default` overloads carrying the ref, so an
implementor outside this repo still compiles. `JSonConstraintTypeAdapter` ignores aliases — a serialized
constraint crosses the wire unqualified, which is right, since an alias only means something inside the
query that created it.

### Constraints

`Constraints` is a factory of immutable `Constraint` trees (`constraints/impl/`), overloaded per field type
purely so IDE completion offers the right value type. Rendering to SQL and binding values are two separate
visitors: `WhereClauseConstraintVisitor` writes the `WHERE` text, `ValueConstraintVisitor` pushes values into
the `PreparedStatement`. `JSonConstraintTypeAdapter` serializes the tree to/from JSON so constraints can
cross a network boundary (a `FieldResolver` maps `{type, name}` back to `Field`s).

`InClause.placeholderCount` is shared by `WhereClauseConstraintVisitor` (which writes the placeholders) and
`ValueConstraintVisitor` (which binds them): they have to agree or the statement goes out with unbound
parameters, which is exactly what `visitNotIn` did — it wrote one placeholder per value and bound none.
Padding repeats a value from the set rather than using NULL, because `x NOT IN (1, NULL)` is never true.

The kinds added after the interface was published — `NotConstraint`, `BetweenConstraint`,
`ExistsConstraint`, `InSubQueryConstraint` — are `default` methods on `ConstraintVisitor` that **throw**.
That keeps a visitor written elsewhere compiling, and makes one that meets a node it cannot render fail
loudly rather than drop the condition and silently return the wrong rows. Follow that pattern for the next
one rather than adding an abstract method.

A subquery renders through `WhereClauseConstraintVisitor.appendSubQuery`, which swaps the `ColumnQualifier`
for one where a bare field of the subquery's own type means the subquery and everything else delegates
outwards — that delegation is what makes it correlated. Alias mode is now driven by
`spec.rootTable() != null` rather than by the join list, since a subquery needs aliases without joining
anything. `JSonConstraintTypeAdapter` refuses to serialize a subquery: its alias is meaningless outside the
query that created it.

**Adding a constraint kind touches five places**: the `impl` class, the `Constraints` factory overloads,
`ConstraintVisitor`, every driver's `WhereClauseConstraintVisitor` subclass, and both directions of
`JSonConstraintTypeAdapter` (plus a case in `JSonConstraintTypeAdapterTest`).

### Adding or fixing a dialect

A driver is a small set of overrides, not a fork. Everything a SELECT is built from travels as one
`SelectQuerySpec`, so a dialect's `SelectQuery` is a constructor forwarding it and a `getQuery()` of one
line — and a new query setting is a component on the record plus the line in `SqlQueryBuilder.spec()` that
fills it, not a new parameter in five query classes and five builders. The pre-record constructors are kept,
deprecated, so a dialect written outside this repo still compiles.

For dialect `X`, `drivers/x/` holds:

| Piece | Purpose |
| --- | --- |
| `XConnection extends JdbcConnection` | implements `getFieldVisitorCreator` → an anonymous `SqlFieldCreationVisitor` mapping field kinds to column types (`BIGSERIAL`, `VARCHAR2`, `LONGVARCHAR`, …) and the auto-increment keyword; may override `endOfRequest` (Oracle takes no trailing `;`) and `addColumn` (HSQLDB can't add several columns at once) |
| `XSelectQuery extends SqlSelectQuery` | usually only `getWhereConstraintVisitor` |
| `impl/XWhereClauseConstraintVisitor` | dialect-specific operators (Postgres `~`/`~*` regex) |
| `request/XSqlQueryBuilder extends SqlQueryBuilder` | returns the dialect's `SelectQuery` |
| a `NamingMapping` | table/column name rendering |

Wiring: `JdbcSqlService.loadDriver()` dispatches on the JDBC URL prefix; `DataSourceSqlService` and
`MappingHelper` dispatch on the `DbType` enum. A new backend means adding a case to **all** of them.

### Naming and escaping

`NamingMapping` turns a `GlobType`/`Field` into a table/column name, honouring `DbTableName`/`DbFieldName`
annotations first. The `boolean escaped` parameter is not cosmetic: `ToPostgreCaseNamingMapping` only quotes
identifiers when `escaped` is true (Postgres folds unquoted names to lower case), and
`AbstractSqlService.toSqlName` upper-cases + mangles reserved words (`COUNT` → `COUNT__`, because HSQLDB > 1.8
rejects leading underscores). Passing the wrong `escaped` value is the usual cause of "column not found" on
Postgres only.

**The rule: `escaped=true` is for text written into SQL, `escaped=false` for a name compared against what
the database stores** — `ResultSetMetaData.getColumnName`, `DatabaseMetaData.getTables`, the field names of
an extracted `GlobType`, anything handed to `extractType`. Getting that backwards is invisible on HSQLDB,
whose mapping ignores `escaped` entirely, and breaks on Postgres only.

Both dispatches now resolve the mapping through `MappingHelper.get(DbType)` — `JdbcSqlService.getMapping`
used to hardcode its own, disagreeing with `MappingHelper` on Postgres. An explicit `NamingMapping` passed
to `JdbcSqlService` wins over the dialect default (it used to be dropped for every recognised dialect); that
is the opt-out for a database created before Postgres became case-aware, and the only reason
`DefaultNamingMapping` is public.

### Composite / array fields

`GlobField`, `GlobArrayField`, `GlobUnionField`, `GlobArrayUnionField` and the array fields have no SQL
equivalent: they are stored as **JSON text** in a long-string column, encoded/decoded with `GSonUtils` from
`globs-gson` (that is why `globs-gson` is a compile dependency). Reading them back needs a
`GlobTypeResolver`. Date/time handling is annotation-driven rather than type-driven: an `IntegerField`/
`LongField` carrying `IsDate`, `IsDateTime` (core) or `IsTimestamp` (this repo) becomes `DATE`/`DATETIME`/
`TIMESTAMP`, so the same three annotations must be handled in `SqlFieldCreationVisitor`,
`SqlValueFieldVisitor` and the accessor-creation code in `SqlQueryBuilder` — miss one and values round-trip
as raw numbers.

### Upsert

`CreateBuilder.onConflictUpdate` / `onConflictDoNothing` put an `Upsert` on the request; `Upsert.resolve`
fills in the defaults once the inserted columns are known (key fields as conflict target, every other
inserted column as the update list) and collapses an empty update list into a do-nothing.
`SqlCreateRequest` then asks the driver for the **whole** statement rather than appending a clause: two
dialects write a `MERGE`, which is not an `INSERT` at all. `JdbcConnection.upsertRequest` is the seam and
throws by default, so a new backend fails loudly instead of silently emitting the wrong dialect;
`mergeRequest` and `insertPart` are the shared halves, and `mergeSource` is what HSQLDB (a `VALUES` row)
and Oracle (a `SELECT ... FROM dual`, having no row constructor) differ on.

The invariant that keeps the value binding untouched: **every form has one placeholder per inserted column,
in the order given**. Break it and `updateStatement` binds the wrong values with no error.

Only the PostgreSQL and HSQLDB paths are executed by the suite. MySQL and Oracle are checked on the
generated SQL, by building their `JdbcConnection` over the test database's connection and reading the
string back — the trick `MysqlSelectQueryTest` and `OracleSelectQueryTest` already use.

### Indexes and foreign keys

`createTable` ends with `createIndexes(globType)`, which reads `globType.getIndices()` (core's index model)
*and* any `DbIndex` annotation, and creates what `DatabaseMetaData.getIndexInfo` says is missing — so it is
idempotent, and `createIndexes` is public for adding indexes to a table that already exists. Index names are
qualified with the table (`<table>_<declared name>`) because Postgres and HSQLDB scope index names to the
schema, not the table.

`DbRef` remains dead: it carries only a target type name, with no column mapping, and core keeps links in a
separate `GlobLinkModel` that a `GlobType` does not expose. Generating foreign keys needs that gap closed
first, plus an answer for creation order and for existing dangling rows. Don't wire it up casually.

### Native column types

`IsUuid`, `DbJson` and `DbColumnType` make a column the database's own type instead of a string. Two halves:

- the DDL, decided by `SqlFieldCreationVisitor.nativeColumnType` and applied **inside `add(...)`**, not in
  each visit method — PostgreSQL and Oracle both override `visitString`, so a check placed there is bypassed
  by exactly the dialects that matter. Dialects override `getUuidType` / `getJsonType`;
- the binding, `NativeValueBinder`, reached through `sqlService.getNativeValueBinder()` so every value
  visitor can get it without new constructor plumbing. PostgreSQL refuses `setString` into a non-text column
  and needs the parameter sent untyped (`setObject(i, v, Types.OTHER)`), which the server then reads as
  whatever the column is; the other dialects keep `setString`. `MappingHelper.nativeValueBinder(DbType)` is
  the single place that decides.

Reading back needs nothing: `getString` on a `uuid` or `jsonb` column returns the text. Arrays are
deliberately left out — `ResultSet.getArray` would need a different accessor, and `SqlQueryBuilder`, which
creates them, has no handle on the dialect.

`createTableRequest(GlobType)` is public so a dialect's column types can be asserted without an instance of
that database, the way `upsertRequest` already was.

### Annotations

One file per annotation, a Glob type (`DbFieldName.java`), registered in
`annotations/AllSqlAnnotations.MODEL`. Adding one means the file **and** the registry, otherwise
JSON-serialized types lose it.

The `@interface` half the workspace used to pair with each annotation (`DbFieldName_.java`) is gone from
this repo — don't write one.
