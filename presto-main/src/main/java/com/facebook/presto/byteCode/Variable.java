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
    private final String name;
    private final int slot;
    private final ParameterizedType type;

    public Variable(String name, int slot, ParameterizedType type)
    {
        this.name = checkNotNull(name, "name is null");
        this.slot = slot;
        this.type = checkNotNull(type, "type is null");
    }

    public ByteCodeNode getValue()
    {
        return VariableInstruction.loadVariable(this);
    }

    public String getName()
    {
        return name;
    }

    public int getSlot()
    {
        return slot;
    }

    public ParameterizedType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
