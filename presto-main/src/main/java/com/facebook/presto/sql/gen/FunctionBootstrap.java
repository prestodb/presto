package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.google.common.collect.Lists;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodType;

import static com.facebook.presto.sql.tree.QualifiedName.parseQualifiedName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FunctionBootstrap
{
    private final Metadata metadata;

    public FunctionBootstrap(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public CallSite bootstrap(String name, MethodType type)
    {
        FunctionInfo function = metadata.getFunction(parseQualifiedName(name), Lists.transform(type.parameterList(), ExpressionCompiler.toTupleType()));
        checkArgument(function != null, "Unknown function %s%s", name, type.parameterList());

        return new ConstantCallSite(function.getScalarFunction());
    }
}
