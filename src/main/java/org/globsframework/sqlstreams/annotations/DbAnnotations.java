package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.annotations.IsDate;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.metamodel.impl.DefaultGlobModel;

public class DbAnnotations {
    public final static GlobModel MODEL =
            new DefaultGlobModel(DbFieldName.TYPE, DbRef.TYPE, IsBigDecimal.TYPE, DbIndex.TYPE, IsDbKey.TYPE, TargetTypeName.TYPE,
                    DbFieldSqlType.TYPE, DbFieldIndex.TYPE, DbFieldIsNullable.TYPE, DbFieldMaxCharSize.TYPE, DbFieldNumericPrecision.TYPE,
                    DbFieldNumericDigit.TYPE, DbFieldMinCharSize.TYPE, IsTimestamp.TYPE, IsDate.TYPE, IsDateTime.TYPE);
}
