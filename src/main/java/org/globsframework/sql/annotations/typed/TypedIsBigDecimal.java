package org.globsframework.sql.annotations.typed;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sql.annotations.IsBigDecimal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target({ElementType.FIELD})
public @interface TypedIsBigDecimal {
    GlobType TYPE = IsBigDecimal.TYPE;

}
