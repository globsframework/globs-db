package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.core.streams.GlobStream;
import org.globsframework.core.streams.accessors.Accessor;

import java.util.Collection;
import java.util.Objects;

public class AccessorGlobBuilder {
    private Field[] fields;
    private Accessor[] accessors;
    private GlobType globType = null;

    private AccessorGlobBuilder(GlobStream globStream, GlobType fallBackType) {
        this(globStream.getFields(), globStream::getAccessor, fallBackType);
    }

    private AccessorGlobBuilder(Collection<Field> fields, FieldAccessor fieldAccessor, GlobType fallBackType) {
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
            this.accessors[i] = Objects.requireNonNull(fieldAccessor.get(field), field.getFullName());
            ++i;
        }
        if (globType == null) {
            globType = fallBackType;
        }
    }

    public interface FieldAccessor {
        Accessor get(Field field);
    }

    public static AccessorGlobBuilder init(Collection<Field> fields, FieldAccessor fieldAccessor, GlobType fallBackType) {
        return new AccessorGlobBuilder(fields, fieldAccessor, fallBackType);
    }

    public static AccessorGlobBuilder init(GlobStream globStream, GlobType fallBackType) {
        return new AccessorGlobBuilder(globStream, fallBackType);
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
