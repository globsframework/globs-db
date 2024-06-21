package org.globsframework.sql.annotations.typed;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sql.annotations.DbRef;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface TypedDbRef {
    String to();
    GlobType TYPE = DbRef.TYPE;

}
