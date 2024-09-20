package org.globsframework.sql.annotations.typed;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.annotations.DbFieldName;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface TypedDbFieldName {
    String value();

    GlobType TYPE = DbFieldName.TYPE;

}
