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

import static com.google.common.base.Preconditions.checkNotNull;

public class Variable
{
    private final LocalVariableDefinition variableDefinition;

    public Variable(LocalVariableDefinition variableDefinition)
    {
        this.variableDefinition = checkNotNull(variableDefinition, "variableDefinition is null");
    }

    public LocalVariableDefinition getLocalVariableDefinition()
    {
        return variableDefinition;
    }

    public ByteCodeNode getValue()
    {
        return VariableInstruction.loadVariable(variableDefinition);
    }

    public ByteCodeNode setValue()
    {
        return VariableInstruction.storeVariable(variableDefinition);
    }
}
