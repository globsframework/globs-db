package org.globsframework.model;

import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.impl.DefaultGlobModel;

public class DummyModel {
    private static final GlobModel MODEL = new DefaultGlobModel(DummyObject.TYPE, DummyObject2.TYPE, DummyObjectWithString.TYPE);

    public static GlobModel get() {
        return MODEL;
    }
}
