package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.DoublePrecision;
import org.globsframework.core.metamodel.annotations.DoublePrecision_;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.annotations.KeyField_;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.StringField;

public class DummyObjectWithString {

    public static GlobType TYPE;

    @KeyField_
    public static StringField ID;

    public static StringField LABEL;

    @DoublePrecision_(4)
    public static DoubleField VALUE;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DummyObjectWithString");
        ID = typeBuilder.declareStringField("id");
        LABEL = typeBuilder.declareStringField("label", KeyField.ZERO);
        VALUE = typeBuilder.declareDoubleField("value", DoublePrecision.create(4));
        TYPE = typeBuilder.build();
    }
}
