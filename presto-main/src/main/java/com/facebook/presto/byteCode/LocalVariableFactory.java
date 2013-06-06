/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

public class LocalVariableFactory
        implements VariableFactory {
    @Override
    public Variable createVariable(CompilerContext context, String name, LocalVariableDefinition variableDefinition)
    {
        return new LocalVariable(variableDefinition);
    }
}
