package org.globsframework.sql.model;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.annotations.Target;
import org.globsframework.metamodel.fields.*;

public class DummyObjectWithGlob {

    public static GlobType TYPE;

    @KeyField
    public static IntegerField ID;

    @Target(DummyObjectWithGlob.class)
    public static GlobField simple;

    @Target(DummyObjectWithGlob.class)
    public static GlobArrayField arrayField;

    static {
        GlobTypeLoaderFactory.create(DummyObjectWithGlob.class, true).load();
    }
}
