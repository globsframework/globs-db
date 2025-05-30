package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.FieldName_;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.BooleanField;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.fields.StringArrayField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.index.Index;
import org.globsframework.core.metamodel.index.impl.DefaultMultiFieldNotUniqueIndex;
import org.globsframework.core.metamodel.index.impl.DefaultMultiFieldUniqueIndex;
import org.globsframework.core.metamodel.index.impl.DefaultNotUniqueIndex;
import org.globsframework.core.metamodel.index.impl.DefaultUniqueIndex;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

import java.util.stream.Stream;

public class DbIndex {
    public static final GlobType TYPE;

    @FieldName_("name")
    public static final StringField NAME;

    @FieldName_("fields")
    public static final StringArrayField FIELDS;

    @FieldName_("isUnique")
    public static final BooleanField IS_UNIQUE;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbIndex");
        TYPE = typeBuilder.unCompleteType();
        NAME = typeBuilder.declareStringField("name");
        FIELDS = typeBuilder.declareStringArrayField("fields");
        IS_UNIQUE = typeBuilder.declareBooleanField("isUnique");
        typeBuilder.complete();
        KEY = KeyBuilder.newEmptyKey(TYPE);

//        GlobTypeLoaderFactory.create(DbIndex.class, "DbIndex")
//                .load();
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
