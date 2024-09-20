package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.annotations.Target;
import org.globsframework.core.metamodel.fields.GlobArrayField;
import org.globsframework.core.metamodel.fields.GlobField;
import org.globsframework.core.metamodel.fields.IntegerField;

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
