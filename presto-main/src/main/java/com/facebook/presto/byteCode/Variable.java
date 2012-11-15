package com.facebook.presto.byteCode;

public interface Variable
{
    LocalVariableDefinition getLocalVariableDefinition();

    ByteCodeNode getValue();
    ByteCodeNode setValue();

    ByteCodeNode getReference();
    ByteCodeNode setReference();

    ByteCodeNode isSet();
    ByteCodeNode unset();

    ByteCodeNode getInitialization();
    ByteCodeNode getCleanup();
}
