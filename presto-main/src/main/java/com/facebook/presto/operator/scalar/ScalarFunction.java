package com.facebook.presto.operator.scalar;

import com.facebook.presto.sql.gen.DefaultFunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ScalarFunction {
    String value() default "";
    String[] alias() default {};
    boolean deterministic() default true;
    Class<? extends FunctionBinder> functionBinder() default DefaultFunctionBinder.class;
}
