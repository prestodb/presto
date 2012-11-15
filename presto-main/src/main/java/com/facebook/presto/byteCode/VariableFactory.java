package com.facebook.presto.byteCode;

public interface VariableFactory
{
    Variable createVariable(CompilerContext context, String name, LocalVariableDefinition variableDefinition);
}
