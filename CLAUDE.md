# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This repo is one clone inside the `globs` workspace — read `../CLAUDE.md` first for the ecosystem-wide
conventions (Glob metamodel, annotation pairs, no-reflection-on-the-hot-path, per-repo release cycles).

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
- **Four tests fail on PostgreSQL today**, all one root cause: `JdbcSqlService.getMapping()` gives Postgres
  `DefaultNamingMapping` (verbatim names, so Postgres folds them to lower case) instead of
  `ToPostgreCaseNamingMapping` (quotes mixed case). `extractType` then cannot find the table it just
  created. Fixing it renames the tables of every existing Postgres user, so do not "fix" it casually — see
  README. The Postgres CI job is `continue-on-error` until then.
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

### Constraints

`Constraints` is a factory of immutable `Constraint` trees (`constraints/impl/`), overloaded per field type
purely so IDE completion offers the right value type. Rendering to SQL and binding values are two separate
visitors: `WhereClauseConstraintVisitor` writes the `WHERE` text, `ValueConstraintVisitor` pushes values into
the `PreparedStatement`. `JSonConstraintTypeAdapter` serializes the tree to/from JSON so constraints can
cross a network boundary (a `FieldResolver` maps `{type, name}` back to `Field`s).

**Adding a constraint kind touches five places**: the `impl` class, the `Constraints` factory overloads,
`ConstraintVisitor`, every driver's `WhereClauseConstraintVisitor` subclass, and both directions of
`JSonConstraintTypeAdapter` (plus a case in `JSonConstraintTypeAdapterTest`).

### Adding or fixing a dialect

A driver is a small set of overrides, not a fork. For dialect `X`, `drivers/x/` holds:

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

### Composite / array fields

`GlobField`, `GlobArrayField`, `GlobUnionField`, `GlobArrayUnionField` and the array fields have no SQL
equivalent: they are stored as **JSON text** in a long-string column, encoded/decoded with `GSonUtils` from
`globs-gson` (that is why `globs-gson` is a compile dependency). Reading them back needs a
`GlobTypeResolver`. Date/time handling is annotation-driven rather than type-driven: an `IntegerField`/
`LongField` carrying `IsDate`, `IsDateTime` (core) or `IsTimestamp` (this repo) becomes `DATE`/`DATETIME`/
`TIMESTAMP`, so the same three annotations must be handled in `SqlFieldCreationVisitor`,
`SqlValueFieldVisitor` and the accessor-creation code in `SqlQueryBuilder` — miss one and values round-trip
as raw numbers.

### Annotations

Same pair convention as the rest of the workspace (`DbFieldName.java` Glob type + `DbFieldName_.java`
`@interface`), registered in `annotations/AllSqlAnnotations.MODEL`. Adding one means both files **and** the
registry, otherwise JSON-serialized types lose it.
