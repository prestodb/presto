/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
