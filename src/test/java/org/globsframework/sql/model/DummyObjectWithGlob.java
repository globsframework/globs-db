package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.annotations.KeyField_;
import org.globsframework.core.metamodel.annotations.Target;
import org.globsframework.core.metamodel.fields.GlobArrayField;
import org.globsframework.core.metamodel.fields.GlobField;
import org.globsframework.core.metamodel.fields.IntegerField;

public class DummyObjectWithGlob {

    public static GlobType TYPE;

    @KeyField_
    public static IntegerField ID;

    @Target(DummyObjectWithGlob.class)
    public static GlobField simple;

    @Target(DummyObjectWithGlob.class)
    public static GlobArrayField arrayField;

    static {
        GlobTypeBuilder builder = GlobTypeBuilderFactory.create("DummyObjectWithGlob");
        ID = builder.declareIntegerField("ID", KeyField.ZERO);
        simple = builder.declareGlobField("simple", () -> DummyObjectWithGlob.TYPE);
        arrayField = builder.declareGlobArrayField("arrayField", () -> DummyObjectWithGlob.TYPE);
        TYPE = builder.build();
    }
}
