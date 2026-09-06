package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.fields.GlobField;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.sql.annotations.DbColumnType;
import org.globsframework.sql.annotations.DbJson;
import org.globsframework.sql.annotations.IsUuid;

public class DummyNativeTypes {

    public static GlobType TYPE;

    public static IntegerField ID;

    /** stored as uuid on PostgreSQL, CHAR(36) elsewhere */
    public static StringField REFERENCE;

    /** already JSON on the Java side: jsonb on PostgreSQL, JSON on MySQL, text elsewhere */
    public static StringField PAYLOAD;

    /** encoded to JSON by the library, and stored in the native type when there is one */
    public static GlobField<DummyObject2> NESTED;

    /** whatever the user writes goes into the DDL */
    public static StringField VERBATIM;

    static {
        GlobTypeBuilder builder = GlobTypeBuilderFactory.create("dummyNativeTypes");
        ID = builder.declareIntegerField("id", KeyField.ZERO);
        REFERENCE = builder.declareStringField("reference", IsUuid.UNIQUE);
        PAYLOAD = builder.declareStringField("payload", DbJson.UNIQUE);
        NESTED = builder.declareGlobField("nested", () -> DummyObject2.TYPE, DbJson.UNIQUE);
        VERBATIM = builder.declareStringField("verbatim", DbColumnType.create("VARCHAR(64)"));
        TYPE = builder.build();
    }
}
