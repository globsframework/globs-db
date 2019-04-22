package org.globsframework.sqlstreams.model;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoader;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.MutableGlobLinkModel;
import org.globsframework.metamodel.annotations.*;
import org.globsframework.metamodel.fields.*;
import org.globsframework.metamodel.index.NotUniqueIndex;
import org.globsframework.metamodel.links.DirectLink;
import org.globsframework.sqlstreams.annotations.typed.TypedIsDate;
import org.globsframework.sqlstreams.annotations.typed.TypedIsDateTime;

public class DummyObject {

    public static GlobType TYPE;

    @KeyField
    @AutoIncrement
    public static IntegerField ID;

    @NamingField
    public static StringField NAME;

    public static DoubleField VALUE;
    public static IntegerField COUNT;
    public static BooleanField PRESENT;

    @TypedIsDate
    public static IntegerField DATE;

    @TypedIsDateTime
    public static LongField CREATED_AT;

    public static BlobField PASSWORD;

    @ContainmentLink
    public static IntegerField LINK_ID;

    public static DirectLink LINK;

    @Target(DummyObject2.class)
    public static IntegerField LINK2_ID;

    public static DirectLink LINK2;

    //  public static UniqueIndex NAME_INDEX;
    public static NotUniqueIndex DATE_INDEX;

    static {
        GlobTypeLoader loader = GlobTypeLoaderFactory.create(DummyObject.class)
              .register(MutableGlobLinkModel.LinkRegister.class,
                    mutableGlobLinkModel -> {
                        LINK = mutableGlobLinkModel.getDirectLinkBuilder(LINK)
                              .add(LINK_ID, DummyObject.ID)
                              .publish();
                        LINK2 = mutableGlobLinkModel.getDirectLinkBuilder(LINK2)
                              .add(LINK2_ID, DummyObject2.ID)
                              .publish();
                    })
              .load();
//    loader.defineUniqueIndex(NAME_INDEX, NAME);
        loader.defineNonUniqueIndex(DATE_INDEX, DATE);
    }
}