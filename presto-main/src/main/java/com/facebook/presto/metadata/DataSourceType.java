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
package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public enum DataSourceType
{
    TPCH("tpch"), // only used in tests.
    NATIVE("native"),
    INTERNAL("internal"),
    IMPORT("import"),
    REMOTE("remote"),
    COLLOCATED("collocated");

    private static final Map<String, DataSourceType> NAME_MAP;

    static {
        ImmutableMap.Builder<String, DataSourceType> builder = ImmutableMap.builder();
        for (DataSourceType dataSourceType : DataSourceType.values()) {
            builder.put(dataSourceType.getName(), dataSourceType);
        }
        NAME_MAP = builder.build();
    }

    private final String name;

    private DataSourceType(String name)
    {
        this.name = checkNotNull(name, "name is null");
    }

    @JsonValue
    public String getName()
    {
        return name;
    }

    @JsonCreator
    public static DataSourceType fromName(String name)
    {
        checkNotNull(name, "name is null");
        DataSourceType dataSourceType = NAME_MAP.get(name);
        checkArgument(dataSourceType != null, "Invalid dataSourceType name: %s", name);
        return dataSourceType;
    }
}
