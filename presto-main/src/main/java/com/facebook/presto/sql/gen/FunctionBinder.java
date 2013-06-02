package com.facebook.presto.sql.gen;

import com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode;
import com.facebook.presto.sql.tree.QualifiedName;

import java.lang.invoke.MethodHandle;
import java.util.List;

public interface FunctionBinder
{
    FunctionBinding bindFunction(long bindingId, QualifiedName name, List<TypedByteCodeNode> arguments, MethodHandle methodHandle);
}
