package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.MutableGlobLinkModel;
import org.globsframework.core.metamodel.annotations.*;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.metamodel.index.NotUniqueIndex;
import org.globsframework.core.metamodel.links.DirectLink;
import org.globsframework.sql.annotations.DbIsNullable;
import org.globsframework.sql.annotations.DbIsNullable_;

public class DummyObject {

    public static final GlobType TYPE;

    @KeyField_
    @AutoIncrement_
    public static IntegerField ID;

    @NamingField_
    public static StringField NAME;

    public static StringArrayField ALIAS;

    public static LongArrayField IDS;

    public static DoubleField VALUE;

    @DbIsNullable_
    public static IntegerField COUNT;

    public static BooleanField PRESENT;

    @IsDate_
    public static IntegerField DATE;

    @IsDateTime_
    public static LongField CREATED_AT;

    public static BytesField PASSWORD;

    public static DateTimeField REAL_DATE_TIME;

    @ContainmentLink_
    public static IntegerField LINK_ID;

    public static DirectLink LINK;

    @Target(DummyObject2.class)
    public static IntegerField LINK2_ID;

    public static DirectLink LINK2;

    //  public static UniqueIndex NAME_INDEX;
    public static NotUniqueIndex DATE_INDEX;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("dummyObject");
        ID = typeBuilder.declareIntegerField("id", KeyField.ZERO, AutoIncrement.INSTANCE);
        NAME = typeBuilder.declareStringField("name", NamingField.UNIQUE_GLOB);
        ALIAS = typeBuilder.declareStringArrayField("alias");
        IDS = typeBuilder.declareLongArrayField("ids");
        VALUE = typeBuilder.declareDoubleField("value");
        COUNT = typeBuilder.declareIntegerField("count", DbIsNullable.nullable());
        PRESENT = typeBuilder.declareBooleanField("present");
        DATE = typeBuilder.declareIntegerField("date", IsDate.UNIQUE);
        CREATED_AT = typeBuilder.declareLongField("createdAt", IsDateTime.UNIQUE);
        PASSWORD = typeBuilder.declareBytesField("password");
        REAL_DATE_TIME = typeBuilder.declareDateTimeField("realDateTime");
        LINK_ID = typeBuilder.declareIntegerField("linkId");
        LINK2_ID = typeBuilder.declareIntegerField("link2Id");

        typeBuilder.register(MutableGlobLinkModel.LinkRegister.class,
                mutableGlobLinkModel -> {
                    LINK = mutableGlobLinkModel.getDirectLinkBuilder(null, "link")
                            .add(LINK_ID, DummyObject.ID)
                            .publish();
                    LINK2 = mutableGlobLinkModel.getDirectLinkBuilder(null, "link2")
                            .add(LINK2_ID, DummyObject2.ID)
                            .publish();
                });
        DATE_INDEX = typeBuilder.addNotUniqueIndex("idsIndex", DATE);
        TYPE = typeBuilder.build();
    }
}
