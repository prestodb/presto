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
package com.facebook.presto.plugin.jdbc;

public class ColumnInfo
{
    private String name;
    private String type;
    private boolean isAutoIncrement;
    private boolean isNullable;

    public ColumnInfo(String name, String type, boolean isAutoIncrement)
    {
        this(name, type, isAutoIncrement, true);
    }

    public ColumnInfo(String name, String type, boolean isAutoIncrement, boolean isNullable)
    {
        this.name = name;
        this.type = type;
        this.isAutoIncrement = isAutoIncrement;
        this.isNullable = isNullable;
    }

    public String getName()
    {
        return name;
    }

    public String getType()
    {
        return type;
    }

    public boolean isAutoIncrement()
    {
        return isAutoIncrement;
    }

    public boolean isNullable()
    {
        return isNullable;
    }
}
