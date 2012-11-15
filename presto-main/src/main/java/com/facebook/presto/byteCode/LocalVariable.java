/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.byteCode;

import com.facebook.presto.byteCode.instruction.VariableInstruction;

import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;

public class LocalVariable
        implements Variable
{
    private final LocalVariableDefinition variableDefinition;

    public LocalVariable(LocalVariableDefinition variableDefinition)
    {
        this.variableDefinition = variableDefinition;
    }

    @Override
    public LocalVariableDefinition getLocalVariableDefinition()
    {
        return variableDefinition;
    }

    @Override
    public ByteCodeNode getValue()
    {
        return VariableInstruction.loadVariable(variableDefinition);
    }

    @Override
    public ByteCodeNode setValue()
    {
        return VariableInstruction.storeVariable(variableDefinition);
    }

    @Override
    public ByteCodeNode getReference()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteCodeNode setReference()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteCodeNode isSet()
    {
        return loadBoolean(true);
    }

    @Override
    public ByteCodeNode unset()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteCodeNode getInitialization()
    {
        return OpCodes.NOP;
    }

    @Override
    public ByteCodeNode getCleanup()
    {
        return OpCodes.NOP;
    }
}
