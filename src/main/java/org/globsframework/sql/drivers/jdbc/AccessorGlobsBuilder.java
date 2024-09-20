package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.core.streams.DbStream;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.core.utils.collections.MultiMap;
import org.globsframework.core.utils.collections.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AccessorGlobsBuilder {
    private MultiMap<GlobType, Pair<Field, Accessor>> accessors = new MultiMap<GlobType, Pair<Field, Accessor>>();

    public AccessorGlobsBuilder(DbStream dbStream) {
        for (Field field : dbStream.getFields()) {
            accessors.put(field.getGlobType(), new Pair<Field, Accessor>(field, dbStream.getAccessor(field)));
        }
    }

    public static AccessorGlobsBuilder init(DbStream dbStream) {
        return new AccessorGlobsBuilder(dbStream);
    }

    public List<Glob> getGlobs() {
        List globs = new ArrayList();
        for (Map.Entry<GlobType, List<Pair<Field, Accessor>>> entry : accessors.entries()) {
            MutableGlob defaultGlob = entry.getKey().instantiate();
            for (Pair<Field, Accessor> pair : entry.getValue()) {
                defaultGlob.setValue(pair.getFirst(), pair.getSecond().getObjectValue());
            }
            globs.add(defaultGlob);
        }
        return globs;
    }
}
