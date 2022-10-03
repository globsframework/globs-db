package org.globsframework.sqlstreams.model;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.fields.DateField;
import org.globsframework.metamodel.fields.DateTimeField;
import org.globsframework.metamodel.fields.StringField;

public class DummyWithDateTime {
    public static GlobType TYPE;

    public static StringField uuid;

    public static DateTimeField created;

    public static DateField date;

    static {
        GlobTypeLoaderFactory.create(DummyWithDateTime.class).load();
    }
}
