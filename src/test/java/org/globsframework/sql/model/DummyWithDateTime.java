package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.DateField;
import org.globsframework.core.metamodel.fields.DateTimeField;
import org.globsframework.core.metamodel.fields.StringField;

public class DummyWithDateTime {
    public static GlobType TYPE;

    public static StringField uuid;

    public static DateTimeField created;

    public static DateField date;

    static {
        GlobTypeBuilder builder = GlobTypeBuilderFactory.create("DummyWithDateTime");
        uuid = builder.declareStringField("uuid");
        created = builder.declareDateTimeField("created");
        date = builder.declareDateField("date");
        TYPE = builder.build();
    }
}
