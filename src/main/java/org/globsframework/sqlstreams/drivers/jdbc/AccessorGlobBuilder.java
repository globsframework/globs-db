package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.streams.DbStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.Check;
import org.globsframework.utils.collections.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class AccessorGlobBuilder {
    private Field[] fields;
    private Accessor[] accessors;
    private GlobType globType = null;

    private AccessorGlobBuilder(DbStream dbStream) {
        this(dbStream.getFields(), dbStream::getAccessor);
    }

    private AccessorGlobBuilder(Collection<Field> fields, FieldAccessor fieldAccessor) {
        this.fields = new Field[fields.size()];
        this.accessors = new Accessor[fields.size()];
        int i = 0;
        for (Field field : fields) {
            GlobType type = field.getGlobType();
            if (globType != null && type != globType) {
                throw new RuntimeException("Different globType in same stream " + type.getName() + " and " + globType.getName());
            }
            globType = type;
            this.fields[i] = field;
            this.accessors[i] = Check.requireNonNull(fieldAccessor.get(field), field);
            ++i;
        }
    }

    public interface FieldAccessor {
        Accessor get(Field field);
    }

    public static AccessorGlobBuilder init(Collection<Field> fields, FieldAccessor fieldAccessor) {
        return new AccessorGlobBuilder(fields, fieldAccessor);
    }

    public static AccessorGlobBuilder init(DbStream dbStream) {
        return new AccessorGlobBuilder(dbStream);
    }

    public Glob getGlob() {
        MutableGlob mutableGlob = globType.instantiate();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            Accessor accessor = accessors[i];
            mutableGlob.setValue(field, accessor.getObjectValue());
        }
        return mutableGlob;
    }
}
