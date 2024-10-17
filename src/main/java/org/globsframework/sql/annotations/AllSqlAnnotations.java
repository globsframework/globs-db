package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.impl.DefaultGlobModel;

public class AllSqlAnnotations {
    public final static GlobModel MODEL =
            new DefaultGlobModel(DbFieldName.TYPE, DbRef.TYPE, IsBigDecimal.TYPE, DbIndex.TYPE, IsDbKey.TYPE, DbTableName.TYPE,
                    DbSqlType.TYPE, DbFieldIndex.TYPE, DbIsNullable.TYPE, DbMaxCharSize.TYPE, DbNumericPrecision.TYPE,
                    DbNumericDigit.TYPE, DbMinCharSize.TYPE, IsTimestamp.TYPE, IsDate.TYPE, IsDateTime.TYPE);
}
