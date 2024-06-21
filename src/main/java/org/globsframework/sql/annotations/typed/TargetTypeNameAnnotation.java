package org.globsframework.sql.annotations.typed;

import org.globsframework.sql.annotations.TargetTypeName;
import org.globsframework.metamodel.GlobType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface TargetTypeNameAnnotation {

    String value();

    GlobType TYPE = TargetTypeName.TYPE;
}
