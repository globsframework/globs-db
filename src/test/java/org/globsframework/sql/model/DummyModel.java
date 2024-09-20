package org.globsframework.sql.model;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.impl.DefaultGlobModel;

public class DummyModel {
    private static final GlobModel MODEL = new DefaultGlobModel(DummyObject.TYPE, DummyObject2.TYPE, DummyObjectWithString.TYPE);

    public static GlobModel get() {
        return MODEL;
    }
}
