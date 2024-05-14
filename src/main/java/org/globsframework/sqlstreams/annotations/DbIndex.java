package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.BooleanField;
import org.globsframework.metamodel.fields.StringArrayField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.index.Index;
import org.globsframework.metamodel.index.impl.DefaultMultiFieldNotUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultMultiFieldUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultNotUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultUniqueIndex;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;

import java.util.stream.Stream;

public class DbIndex {
    public static GlobType TYPE;

    @FieldNameAnnotation("name")
    public static StringField NAME;

    @FieldNameAnnotation("fields")
    public static StringArrayField FIELDS;

    @FieldNameAnnotation("isUnique")
    public static BooleanField IS_UNIQUE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbIndex.class, "DbIndex")
                .load();
    }

    static public Index createIndex(GlobType globType, Glob index) {
        Field[] fields = Stream.of(index.get(FIELDS))
                .map(globType::getField)
                .toArray(Field[]::new);

        if (index.size() == 1) {
            if (index.get(IS_UNIQUE, Boolean.TRUE)) {
                return new DefaultUniqueIndex(index.get(NAME), fields[0]);
            } else {
                return new DefaultNotUniqueIndex(index.get(NAME), fields[0]);
            }
        } else {
            if (index.get(IS_UNIQUE, Boolean.TRUE)) {
                return new DefaultMultiFieldUniqueIndex(index.get(NAME), fields);
            } else {
                return new DefaultMultiFieldNotUniqueIndex(index.get(NAME), fields);
            }
        }
    }
}
