package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.type.DataType;

import java.util.Set;

public interface GlobTypeExtractor {
    GlobTypeExtractor columnToIgnore(Set<String> columnToIgnore);

    GlobTypeExtractor forceType(Transtype transtype);

    GlobTypeExtractor forceType(String fieldName, DataType source);

    GlobType extract();


    interface Transtype {
        DataType getType(String name, DataType sqlType);
    }
}
