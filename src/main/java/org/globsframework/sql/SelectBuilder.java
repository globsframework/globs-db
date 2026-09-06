package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.streams.accessors.*;
import org.globsframework.core.utils.Ref;
import org.globsframework.sql.constraints.Constraint;

import java.time.Duration;

public interface SelectBuilder {

    SelectQuery getQuery();

    SelectQuery getQuery(String sql);

    SelectQuery getNotAutoCloseQuery();

    SelectBuilder select(Field field);

    SelectBuilder selectAll();

    // ----------------------------------------------------------------------------------------
    // Joins. Without them a query names one table -- or several, cross joined, if constraints
    // reach across types -- and every column is written with its table name, as before.
    // ----------------------------------------------------------------------------------------

    /**
     * The occurrence of the type this builder was opened on.
     */
    TableRef rootTable();

    /**
     * Declares a further occurrence of a table, with its own alias. Calling it twice on the same
     * type gives two aliases, which is what a self join needs.
     */
    TableRef table(GlobType type);

    /**
     * @param table the occurrence to join, from {@link #table(GlobType)}
     * @param on    the join condition, normally comparing a column of an already joined table with
     *              a column of this one
     */
    SelectBuilder innerJoin(TableRef table, Constraint on);

    SelectBuilder leftJoin(TableRef table, Constraint on);

    /**
     * Selects a column of a named occurrence into the globs {@code executeAsGlobs} builds.
     * <p>
     * A glob has one type and one value per field, which bounds what it can carry out of a join:
     * columns of a <em>joined</em> table do not fit in it, and neither do both sides of a self join.
     * Read those with {@link #retrieveUnTyped(ColumnRef)} — or with the plain {@code retrieve} /
     * {@code select(field, Ref)} accessors, which a join changes nothing about.
     */
    SelectBuilder select(ColumnRef column);

    /**
     * An accessor on a column of a named occurrence, outside the globs being built: how a joined
     * table's columns, and both sides of a self join, are read.
     */
    Accessor retrieveUnTyped(ColumnRef column);

    SelectBuilder select(IntegerField field, Ref<IntegerAccessor> accessor);

    SelectBuilder select(LongField field, Ref<LongAccessor> accessor);

    SelectBuilder select(BooleanField field, Ref<BooleanAccessor> accessor);

    SelectBuilder select(StringField field, Ref<StringAccessor> accessor);

    SelectBuilder select(DoubleField field, Ref<DoubleAccessor> accessor);

    SelectBuilder select(DateTimeField field, Ref<DateTimeAccessor> accessor);

    SelectBuilder select(DateField field, Ref<DateAccessor> accessor);

    SelectBuilder select(BytesField field, Ref<BytesAccessor> accessor);

    SelectBuilder select(GlobField<?> field, Ref<GlobAccessor> accessor);

    SelectBuilder select(GlobArrayField<?> field, Ref<GlobsAccessor> accessor);


    SelectBuilder select(LongArrayField field, Ref<LongArrayAccessor> accessor);

    SelectBuilder groupBy(Field field);

    SelectBuilder groupBy(ColumnRef column);

    SelectBuilder orderAsc(Field field);

    SelectBuilder orderDesc(Field field);

    SelectBuilder orderAsc(ColumnRef column);

    SelectBuilder orderDesc(ColumnRef column);

    SelectBuilder top(int n);

    SelectBuilder skip(int n);

    /**
     * Rows the driver buffers per round trip. Left to the driver's own default when unset, or to the
     * service-wide default when one is configured.
     * <p>
     * It is what makes a large result set streamable instead of materialised in the client — but on
     * PostgreSQL only inside a transaction: on an auto-commit connection the driver reads every row
     * whatever the fetch size, so {@code read(...)} and {@code getAutoCommitDb()} do not stream.
     */
    SelectBuilder fetchSize(int fetchSize);

    /**
     * How long the statement may run before the database cancels it, raising a
     * {@code QueryCanceled}. JDBC counts whole seconds, so anything shorter is rounded up to one.
     */
    SelectBuilder queryTimeout(Duration queryTimeout);

    SelectBuilder withKeys();

    IntegerAccessor max(IntegerField field);

    LongAccessor max(LongField field);

    LongAccessor count(Field field);

    LongAccessor count();

    default LongAccessor count(IntegerField field) {
        return count(((Field) field));
    }

    default LongAccessor count(LongField field) {
        return count(((Field) field));
    }

    ;

    LongAccessor sum(IntegerField count);

    LongAccessor sum(LongField field);

    IntegerAccessor min(IntegerField field);

    LongAccessor min(LongField field);

    IntegerAccessor retrieve(IntegerField field);

    LongAccessor retrieve(LongField field);

    StringAccessor retrieve(StringField field);

    BooleanAccessor retrieve(BooleanField field);

    DoubleAccessor retrieve(DoubleField field);

    DateTimeAccessor retrieve(DateTimeField field);

    DateAccessor retrieve(DateField field);

    BytesAccessor retrieve(BytesField field);

    StringArrayAccessor retrieve(StringArrayField field);

    IntegerArrayAccessor retrieve(IntegerArrayField field);

    LongArrayAccessor retrieve(LongArrayField field);

    DoubleArrayAccessor retrieve(DoubleArrayField field);

    BooleanArrayAccessor retrieve(BooleanArrayField field);

    GlobAccessor retrieve(GlobField<?> field);

    GlobAccessor retrieve(GlobUnionField field);

    GlobsAccessor retrieve(GlobArrayField<?> field);

    GlobsAccessor retrieve(GlobArrayUnionField field);

    Accessor retrieveUnTyped(Field field);

}
