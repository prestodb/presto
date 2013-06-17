package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.sql.gen.ExpressionCompiler.TypedByteCodeNode;

import java.util.List;

public interface FunctionBinder
{
    FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments);
}
