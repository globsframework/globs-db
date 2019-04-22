package org.globsframework.sqlstreams.annotations.typed;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.annotations.IsTimestamp;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface TypedIsTimestamp {
    GlobType TYPE = IsTimestamp.TYPE;
}
