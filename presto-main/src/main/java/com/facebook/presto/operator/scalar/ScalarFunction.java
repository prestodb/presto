package com.facebook.presto.operator.scalar;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ScalarFunction {
    String value() default "";
    String[] alias() default {};
    boolean deterministic() default true;
}
