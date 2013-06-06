package com.facebook.presto.sql.gen;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

import static java.lang.invoke.MethodHandles.constant;

public class SliceLiteralBootstrap
{
    public static final Method SLICE_LITERAL_BOOTSTRAP;

    static {
        try {
            SLICE_LITERAL_BOOTSTRAP = SliceLiteralBootstrap.class.getMethod("bootstrap", Lookup.class, String.class, MethodType.class, String.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static java.lang.invoke.CallSite bootstrap(Lookup lookup, String name, MethodType type, String value)
    {
        return new ConstantCallSite(constant(Slice.class, Slices.copiedBuffer(value, Charsets.UTF_8)));
    }

}
