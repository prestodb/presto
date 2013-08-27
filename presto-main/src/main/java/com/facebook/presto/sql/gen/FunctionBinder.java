package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;

import java.util.List;

public interface FunctionBinder
{
    FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments);
}
