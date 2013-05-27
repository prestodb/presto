package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.sql.tree.QualifiedName.parseQualifiedName;
import static com.google.common.base.Preconditions.checkArgument;

public class FunctionBootstrap
{
    public static final Method FUNCTION_BOOTSTRAP;

    // todo this is a total hack
    public static final AtomicReference<Metadata> metadataReference = new AtomicReference<>();

    static {
        try {
            FUNCTION_BOOTSTRAP = FunctionBootstrap.class.getMethod("bootstrap", Lookup.class, String.class, MethodType.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static java.lang.invoke.CallSite bootstrap(Lookup lookup, String name, MethodType type)
    {
        Metadata metadata = metadataReference.get();
        FunctionInfo function = metadata.getFunction(parseQualifiedName(name), Lists.transform(type.parameterList(), ExpressionCompiler.toTupleType()));
        checkArgument(function != null, "Unknown function %s%s", name, type.parameterList());

        return new ConstantCallSite(function.getScalarFunction());
    }

}
