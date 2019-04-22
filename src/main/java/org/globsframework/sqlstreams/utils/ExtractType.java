package org.globsframework.sqlstreams.utils;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.annotations.*;
import org.globsframework.utils.exceptions.GlobsException;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ExtractType {

    static public GlobType createFromMetaData(ResultSetMetaData resultSetMetaData)  {
        try {
            int columnCount = resultSetMetaData.getColumnCount();

            GlobTypeBuilder globTypeBuilder = new DefaultGlobTypeBuilder("fromResult");

            for (int i = 1; i <= columnCount; i++) {
                int dataType = resultSetMetaData.getColumnType(i);
                String columnName = resultSetMetaData.getColumnLabel(i);

                Glob sqlIndex = DbFieldIndex.create(i);
                Glob sqlName = DbFieldName.create(columnName);
                Glob nullable = DbFieldIsNullable.create(resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNullable);
                Glob sqlType = DbFieldSqlType.create(dataType);
                Glob minSize = null;

                switch (dataType) {
                    case Types.CHAR: {
                        int size = resultSetMetaData.getPrecision(i);
                        if (size != Integer.MAX_VALUE) {
                            minSize = DbFieldMinCharSize.create(size);
                        }
                        //no break;
                    }
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR: {
                        int size = resultSetMetaData.getPrecision(i);
                        Glob maxSize = null;
                        if (size != Integer.MAX_VALUE) {
                            maxSize = DbFieldMaxCharSize.create(size);
                        }
                        StringField field = globTypeBuilder.declareStringField(columnName, maxSize, sqlType, nullable,
                                minSize, sqlName, sqlIndex);
                        break;
                    }
                    case Types.DECIMAL:
                    case Types.NUMERIC: {

                        int size = resultSetMetaData.getPrecision(i);
                        Glob maxSize = DbFieldNumericPrecision.create(size);
                        int decimal = resultSetMetaData.getScale(i);
                        Glob scale = DbFieldNumericDigit.create(decimal);
                        if (decimal == 0) {
                            if (size > 0 && size <= 9) {
                                globTypeBuilder.declareIntegerField(columnName, sqlType, nullable, sqlName, sqlIndex);
                            } else if (size > 0 && size <= 18) {
                                globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex);
                            } else {
                                globTypeBuilder.declareDoubleField(columnName, scale, maxSize, sqlType, nullable, sqlName, sqlIndex);
                            }
                        } else {
                            globTypeBuilder.declareDoubleField(columnName, scale, maxSize, sqlType, nullable,
                                    sqlName, sqlIndex);
                        }
                        break;
                    }
                    case Types.FLOAT:
                    case Types.DOUBLE: {
                        globTypeBuilder.declareDoubleField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.BIT:
                    case Types.BOOLEAN: {
                        globTypeBuilder.declareBooleanField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER: {
                        globTypeBuilder.declareIntegerField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.BIGINT: {
                        globTypeBuilder.declareLongField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB: {
                        globTypeBuilder.declareBlobField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.DATE: {
                        //        case Types.TIME:
                        globTypeBuilder.declareDateField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    case Types.TIMESTAMP: {
                        globTypeBuilder.declareDateTimeField(columnName, sqlType, nullable, sqlName, sqlIndex);
                        break;
                    }
                    default:
                        throw new GlobsException("Type '" + dataType + "' not managed for column " + columnName);
                }
            }

            return globTypeBuilder.get();
        } catch (SQLException e) {
            throw new RuntimeException("Fail to retrieve data from request", e);
        }
    }

}
