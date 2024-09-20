package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.DoublePrecision;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;

public class DummyObject2 {

    public static GlobType TYPE;

    @KeyField
    public static LongField ID;

    public static StringField LABEL;

    @DoublePrecision(4)
    public static DoubleField VALUE;

    static {
        GlobTypeLoaderFactory.create(DummyObject2.class, true).load();
    }
}
