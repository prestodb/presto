package com.facebook.presto.byteCode;

public interface ByteCodeNodeFactory
{
    ByteCodeNode build(CompilerContext context, ExpectedType expectedType);
}
