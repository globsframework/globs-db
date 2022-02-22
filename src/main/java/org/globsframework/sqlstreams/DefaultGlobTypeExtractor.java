package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.annotations.IsDate;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.metamodel.annotations.KeyAnnotationType;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.annotations.*;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.utils.NanoChrono;
import org.globsframework.utils.Strings;
import org.globsframework.utils.exceptions.GlobsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class DefaultGlobTypeExtractor implements GlobTypeExtractor {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private JdbcSqlService sqlService;
    private String tableName;
    private Set<String> columnToIgnore = new HashSet<>();
    private GlobTypeBuilder globTypeBuilder;
    private Map<String, DataType> forcedType = new HashMap<>();
    private Transtype transtype = new Transtype() {
        public DataType getType(String name, DataType sqlType) {
            return null;
        }
    };

    public DefaultGlobTypeExtractor(JdbcSqlService sqlService, String tableName) {
        this.sqlService = sqlService;
        this.tableName = tableName;
        globTypeBuilder = DefaultGlobTypeBuilder.init(tableName);
    }

    public GlobTypeExtractor columnToIgnore(Set<String> columnToIgnore) {
        this.columnToIgnore.addAll(columnToIgnore);
        return this;
    }

    public GlobTypeExtractor forceType(Transtype transtype) {
        this.transtype = transtype;
        return this;
    }

    public GlobTypeExtractor forceType(String fieldName, DataType source) {
        forcedType.put(fieldName, source);
        return this;
    }

    public GlobType extract() {
        return createFrom();
    }

    public GlobType createFrom() {
        if (!completeGlobTypeBuilder()) {
            return null;
        }
        return globTypeBuilder.get();
    }

    private boolean completeGlobTypeBuilder() {
        if (Strings.isNullOrEmpty(tableName)) {
            String msg = "No table received";
            LOGGER.error(msg);
            throw new GlobsException(msg);
        }
        JdbcConnection db = sqlService.getAutoCommitDb();
        try {
            Connection connection = db.getConnection();
            return createFrom(connection, connection.getMetaData(), tableName);
        } catch (SQLException e) {
            LOGGER.error("sql error", e);
            throw db.getTypedException(null, e);
        } finally {
            db.commitAndClose();
        }
    }

    private boolean createFrom(Connection connection, DatabaseMetaData databaseMetaData, String tableName) {
        NanoChrono chrono = NanoChrono.start();
        try {
            String sqlTableName = getEscapedTableName(databaseMetaData, tableName);

            if (!hasTableOrView(connection, databaseMetaData, sqlTableName)) {
                LOGGER.warn("Table not found");
                return false;
            }
            globTypeBuilder.addAnnotation(TargetTypeName.create(tableName));
            SortedMap<String, Integer> primaryKeys = fillPrimaryKeys(connection, tableName, databaseMetaData);
            initColumns(connection, sqlTableName, databaseMetaData, primaryKeys);
        } catch (SQLException e) {
            String message = "For " + tableName;
            LOGGER.error(message, e);
            throw new GlobsException(message, e);
        }
        LOGGER.info("extraction of metadata of " + tableName + " in " + chrono.getElapsedTimeInMS() + "ms.");
        return true;
    }

    protected String getEscapedTableName(DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        return tableName;
    }

    private boolean hasTableOrView(Connection connection, DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        ResultSet tables = databaseMetaData.getTables(connection.getCatalog(), getSchemaName(connection), tableName, new String[]{"TABLE", "VIEW"});
        boolean result = tables.next();
        tables.close();
        return result;
    }


    static class ForeignKey {
        String pkTableName;
        String pkColumnName;
        String fkTableName;
        String fkColumnName;
        String fkName;
        String pkName;
        String keySeq;
    }

    private List<ForeignKey> getFk(Connection connection, DatabaseMetaData databaseMetaData, String tableName) throws SQLException {
        try (ResultSet resultSet = databaseMetaData.getImportedKeys(connection.getCatalog(), getSchemaName(connection), tableName)) {
            List<ForeignKey> foreignKeys = new ArrayList<>();
            while (resultSet.next()) {
                ForeignKey foreignKey = new ForeignKey();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i < columnCount + 1; i++) {
                    String currentColumn = metaData.getColumnName(i);
                    if (currentColumn.equalsIgnoreCase("PKTABLE_NAME")) {
                        foreignKey.pkTableName = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("PKCOLUMN_NAME")) {
                        foreignKey.pkColumnName = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("FKTABLE_NAME")) {
                        foreignKey.fkTableName = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("FKCOLUMN_NAME")) {
                        foreignKey.fkColumnName = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("KEY_SEQ")) {
                        foreignKey.keySeq = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("FK_NAME")) {
                        foreignKey.fkName = resultSet.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("PK_NAME")) {
                        foreignKey.pkName = resultSet.getString(currentColumn);
                    }
                }
                foreignKeys.add(foreignKey);
            }
            return foreignKeys;
        }
    }

    private SortedMap<String, Integer> fillPrimaryKeys(Connection connection, String tableName, DatabaseMetaData databaseMetaData) throws SQLException {
        ResultSet columns = databaseMetaData.getPrimaryKeys(connection.getCatalog(), getSchemaName(connection), tableName);
        try {
            SortedMap<String, Integer> keyFieldByOrder = new TreeMap<>();
            while (columns.next()) {
                ResultSetMetaData metaData = columns.getMetaData();
                int columnCount = metaData.getColumnCount();
                String column_name = null;
                Integer key_seq = null;
                for (int i = 1; i < columnCount + 1; i++) {
                    String currentColumn = metaData.getColumnName(i);
                    if (currentColumn.equalsIgnoreCase("COLUMN_NAME")) {
                        column_name = columns.getString(currentColumn);
                    } else if (currentColumn.equalsIgnoreCase("KEY_SEQ")) {
                        key_seq = columns.getInt(currentColumn);
                    }
                }
                if (column_name == null || key_seq == null) {
                    String message = "At least one of column name " + column_name + " or key sequence " + key_seq + " is null.";
                    LOGGER.error(message);
                    throw new RuntimeException(message);
                }
                keyFieldByOrder.put(column_name, key_seq - 1); // key in glob start at 0
            }
            return keyFieldByOrder;
        } finally {
            columns.close();
        }
    }


    static class ColumnInfo {
        String columnName;
        int dataType;
        int nullable;
        int columnSize;
        boolean columnSizeIsNull;
        int decimalDigits;
        boolean decimalDigitsIsNull;
    }

    protected String getSchemaName(Connection connection) throws SQLException {
        return null;
    }


    private void initColumns(Connection connection, String tableName, DatabaseMetaData databaseMetaData, SortedMap<String, Integer> keys) throws SQLException {

        try (ResultSet columns = databaseMetaData.getColumns(connection.getCatalog(), getSchemaName(connection), tableName, "%")) {
            int index = -1;
            int keyIndex = 0;
            KeyInfo keyInfo = new KeyInfo(keys);
            while (columns.next()) {
                index++;
                Glob sqlIndex = DbFieldIndex.create(index);
                ResultSetMetaData metaData = columns.getMetaData();
                ColumnInfo columnInfo = fillColumnInfo(columns, metaData);
                String columnName = columnInfo.columnName;
                if (columnToIgnore.contains(columnName)) {
                    continue;
                }
                Glob sqlName = DbFieldName.create(columnName);
                int dataType = columnInfo.dataType;
                Glob nullable = DbFieldIsNullable.create(columnInfo.nullable == DatabaseMetaData.columnNullable);
                Glob sqlType = DbFieldSqlType.create(dataType);
                Glob minSize = null;
                switch (dataType) {
                    case Types.CHAR: {
                        int size = columnInfo.columnSize;
                        if (size != Integer.MAX_VALUE) {
                            minSize = DbFieldMinCharSize.create(size);
                        }
                        //no break;
                    }
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR: {
                        int size = columnInfo.columnSize;
                        Glob maxSize = null;
                        if (size != Integer.MAX_VALUE) {
                            maxSize = DbFieldMaxCharSize.create(size);
                        }
                        StringField field = this.globTypeBuilder.declareStringField(columnName, maxSize, sqlType, nullable,
                                minSize, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.DECIMAL:
                    case Types.NUMERIC: {

                        Glob maxSize = null;
                        int size = columnInfo.columnSize;
                        if (size != 0 || !columnInfo.columnSizeIsNull) {
                            maxSize = DbFieldNumericPrecision.create(size);
                        }
                        Glob scale = null;
                        int decimal = columnInfo.decimalDigits;
                        if (!columnInfo.decimalDigitsIsNull) {
                            scale = DbFieldNumericDigit.create(decimal);
                        }
                        if (decimal == 0 && !columnInfo.decimalDigitsIsNull) {
                            Field field;
                            if (!columnInfo.columnSizeIsNull && size > 0 && size <= 9) {
                                field = this.globTypeBuilder.declareIntegerField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                            } else if (!columnInfo.columnSizeIsNull && size > 0 && size <= 18) {
                                field = this.globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                            } else {
                                field = this.globTypeBuilder.declareDoubleField(columnName, scale, maxSize, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                            }
                        } else {
                            DoubleField field = this.globTypeBuilder.declareDoubleField(columnName, scale, maxSize, sqlType, nullable,
                                    sqlName, sqlIndex, keyInfo.invoke(columnName));
                        }
                        break;
                    }
                    case Types.FLOAT:
                    case Types.DOUBLE: {
                        Field field = this.globTypeBuilder.declareDoubleField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.BIT:
                    case Types.BOOLEAN: {
                        Field field = this.globTypeBuilder.declareBooleanField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER: {
                        IntegerField field = this.globTypeBuilder.declareIntegerField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.BIGINT: {
                        Field field = this.globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB: {
                        Field field = this.globTypeBuilder.declareBlobField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        break;
                    }
                    case Types.DATE: {
                        //        case Types.TIME:
                        DataType type1 = forcedType.get(columnName);
                        DataType type2 = transtype.getType(columnName, DataType.Date);
                        DataType wantedType = type1 != null ? type1 : type2 ;
                        if (wantedType != null) {
                            if (wantedType == DataType.Integer) {
                                this.globTypeBuilder.declareIntegerField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName),
                                        IsDate.UNIQUE);
                            } else if (wantedType == DataType.Long) {
                                this.globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName),
                                        IsDate.UNIQUE);
                            } else if (wantedType == DataType.Date) {
                                this.globTypeBuilder.declareDateField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                            } else {
                                String message = "Can not force date to " + wantedType.name();
                                LOGGER.error(message);
                                throw new RuntimeException(message);
                            }
                        } else {
                            this.globTypeBuilder.declareDateField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        }
                        break;
                    }
                    case Types.TIMESTAMP: {
                        DataType type1 = forcedType.get(columnName);
                        DataType type2 = transtype.getType(columnName, DataType.DateTime);
                        DataType wantedType = type1 != null ? type1 : type2 ;
                        if (wantedType != null) {
                            if (wantedType == DataType.Long) {
                                this.globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName),
                                        IsDateTime.UNIQUE);
                            } else if (wantedType == DataType.DateTime) {
                                this.globTypeBuilder.declareDateTimeField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                            } else {
                                String message = "Can not force timestamp to " + wantedType.name();
                                LOGGER.error(message);
                                throw new RuntimeException(message);
                            }
                        } else{
                            this.globTypeBuilder.declareDateTimeField(columnName, sqlType, nullable, sqlName, sqlIndex, keyInfo.invoke(columnName));
                        }
                        break;
                    }
//                    case Types.OTHER:
//                        logger.warn(columnName + " is of type 'other' => ignored");
//                        break;
                    default:
                        String message = "Type '" + dataType + "' not managed for column " + columnName;
                        LOGGER.error(message);
                        throw new GlobsException(message);
                }
            }
        }
    }

    private ColumnInfo fillColumnInfo(ResultSet columns, ResultSetMetaData metaData) throws SQLException {
        ColumnInfo columnInfo = new ColumnInfo();
        int metaDataColumnCount = metaData.getColumnCount();
        for (int i = 1; i < metaDataColumnCount + 1; i++) {
            String currentColumn = metaData.getColumnName(i);
            if (currentColumn.equalsIgnoreCase("COLUMN_NAME")) {
                columnInfo.columnName = columns.getString(i);
            } else if (currentColumn.equalsIgnoreCase("DATA_TYPE")) {
                columnInfo.dataType = columns.getInt(i);
            } else if (currentColumn.equalsIgnoreCase("NULLABLE")) {
                columnInfo.nullable = columns.getInt(i);
            } else if (currentColumn.equalsIgnoreCase("COLUMN_SIZE")) {
                columnInfo.columnSize = columns.getInt(i);
                columnInfo.columnSizeIsNull = columns.wasNull();
            } else if (currentColumn.equalsIgnoreCase("DECIMAL_DIGITS")) {
                columnInfo.decimalDigits = columns.getInt(i);
                columnInfo.decimalDigitsIsNull = columns.wasNull();
            }
        }
        return columnInfo;
    }

    private class KeyInfo {
        private SortedMap<String, Integer> keys;
        private int keyIndex;
        private String columnName;

        public KeyInfo(SortedMap<String, Integer> keys) {
            this.keys = keys;
        }

        public Glob invoke(String columnName) {
            Integer indexOfKeyField = keys.get(columnName);
            return indexOfKeyField != null ? KeyAnnotationType.create(indexOfKeyField) : null;
        }
    }
}
