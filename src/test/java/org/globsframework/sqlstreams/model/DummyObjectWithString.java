package org.globsframework.sqlstreams.model;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.DoublePrecision;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.StringField;

public class DummyObjectWithString {

    public static GlobType TYPE;

    @KeyField
    public static StringField ID;

    public static StringField LABEL;

    @DoublePrecision(4)
    public static DoubleField VALUE;

    static {
        GlobTypeLoaderFactory.create(DummyObjectWithString.class, true).load();
    }
}
