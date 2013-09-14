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

import javax.annotation.concurrent.Immutable;

@Immutable
public class LocalVariableDefinition
{
    private final String name;
    private final int slot;
    private final ParameterizedType type;

    public LocalVariableDefinition(String name, int slot, ParameterizedType type)
    {
        this.name = name;
        this.slot = slot;
        this.type = type;
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
        final StringBuilder sb = new StringBuilder();
        sb.append("LocalVariableDefinition");
        sb.append("{name='").append(name).append('\'');
        sb.append(", slot=").append(slot);
        sb.append('}');
        return sb.toString();
    }
}
