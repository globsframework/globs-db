package org.globsframework.sqlstreams.json;

import org.globsframework.metamodel.GlobType;

public interface GlobTypeResolver {
    GlobType get(String name);
}
