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
package com.facebook.presto.sql.analyzer.crux;

public class ColumnSpecification
{
    private final int index;
    private final String name;
    private final Type type;
    private final boolean hidden;
    private final Qualifier qualifier;
    private final boolean isPartition;
    private final boolean deprecated;

    public ColumnSpecification(int index, String name, Type type, boolean hidden, Qualifier qualifier, boolean isPartition, boolean deprecated)
    {
        this.index = index;
        this.name = name;
        this.type = type;
        this.hidden = hidden;
        this.qualifier = qualifier;
        this.isPartition = isPartition;
        this.deprecated = deprecated;
    }

    public int getIndex()
    {
        return index;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public boolean getHidden()
    {
        return hidden;
    }

    public Qualifier getQualifier()
    {
        return qualifier;
    }

    public boolean getIsPartition()
    {
        return isPartition;
    }

    public boolean getDeprecated()
    {
        return deprecated;
    }
}
