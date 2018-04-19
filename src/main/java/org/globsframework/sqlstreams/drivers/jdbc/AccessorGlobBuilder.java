package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.collections.MultiMap;
import org.globsframework.utils.collections.Pair;

import java.util.ArrayList;
import java.util.List;

public class AccessorGlobBuilder {
    private final GlobType type;
    private List<Pair<Field, Accessor>> accessors = new ArrayList<>();

    public AccessorGlobBuilder(GlobStream globStream) {
        this(globStream, null);
    }

    public AccessorGlobBuilder(GlobStream globStream, GlobType globType) {
        for (Field field : globStream.getFields()) {
            if (globType == null) {
                globType = field.getGlobType();
            } else if (globType != field.getGlobType()) {
                throw new RuntimeException("Multiple type " + globType.getName() + " and " + field.getGlobType().getName());
            }
            accessors.add(new Pair<>(field, getAccessor(globStream, field)));
        }
        if (globType == null) {
            throw new RuntimeException("No field selected");
        }
        type = globType;
    }

    public Accessor getAccessor(GlobStream globStream, Field field) {
        return globStream.getAccessor(field);
    }

    public static AccessorGlobBuilder init(GlobStream globStream) {
        return new AccessorGlobBuilder(globStream, null);
    }

    public static AccessorGlobBuilder init(GlobStream globStream, GlobType globType) {
        return new AccessorGlobBuilder(globStream, globType);
    }

    public MutableGlob getGlob() {
        MutableGlob defaultGlob = type.instantiate();
        for (Pair<Field, Accessor> pair : accessors) {
                defaultGlob.setValue(pair.getFirst(), pair.getSecond().getObjectValue());
            }
        return defaultGlob;
    }
}
