package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.index.Index;
import org.globsframework.metamodel.index.impl.DefaultMultiFieldNotUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultMultiFieldUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultNotUniqueIndex;
import org.globsframework.metamodel.index.impl.DefaultUniqueIndex;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DbIndex {
    public static GlobType TYPE;

    @FieldNameAnnotation("name")
    public static StringField NAME;

    @FieldNameAnnotation("field_1")
    public static StringField FIELD_1;

    @FieldNameAnnotation("field_2")
    public static StringField FIELD_2;

    @FieldNameAnnotation("field_3")
    public static StringField FIELD_3;

    @FieldNameAnnotation("field_4")
    public static StringField FIELD_4;

    @FieldNameAnnotation("type")
    public static StringField INDEX_TYPE; // unique notUnique


    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbIndex.class, "DbIndex")
              .load();
    }

    static public Index createIndex(GlobType globType, Glob index) {
        List<Field> fields = new ArrayList<>();
        index.getOpt(FIELD_1).ifPresent(s -> fields.add(globType.getField(s)));
        index.getOpt(FIELD_2).ifPresent(s -> fields.add(globType.getField(s)));
        index.getOpt(FIELD_3).ifPresent(s -> fields.add(globType.getField(s)));
        index.getOpt(FIELD_4).ifPresent(s -> fields.add(globType.getField(s)));

        if (index.size() == 1) {
            if (index.get(INDEX_TYPE, "unique").equals("unique")) {
                return new DefaultUniqueIndex(index.get(NAME), fields.get(0));
            }
            else {
                return new DefaultNotUniqueIndex(index.get(NAME), fields.get(0));
            }
        }else {
            if (index.get(INDEX_TYPE, "unique").equals("unique")) {
                return new DefaultMultiFieldUniqueIndex(index.get(NAME), fields.toArray(new Field[0]));
            }
            else {
                return new DefaultMultiFieldNotUniqueIndex(index.get(NAME), fields.toArray(new Field[0]));
            }
        }
    }
}
