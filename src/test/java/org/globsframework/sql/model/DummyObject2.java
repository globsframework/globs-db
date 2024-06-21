package org.globsframework.sql.model;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.DoublePrecision;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.fields.StringField;

public class DummyObject2 {

    public static GlobType TYPE;

    @KeyField
    public static LongField ID;

    public static StringField LABEL;

    @DoublePrecision(4)
    public static DoubleField VALUE;

    static {
        GlobTypeLoaderFactory.create(DummyObject2.class,true).load();
    }
}
