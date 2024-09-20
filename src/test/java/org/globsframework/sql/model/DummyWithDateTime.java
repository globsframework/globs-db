package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.fields.DateField;
import org.globsframework.core.metamodel.fields.DateTimeField;
import org.globsframework.core.metamodel.fields.StringField;

public class DummyWithDateTime {
    public static GlobType TYPE;

    public static StringField uuid;

    public static DateTimeField created;

    public static DateField date;

    static {
        GlobTypeLoaderFactory.create(DummyWithDateTime.class).load();
    }
}
