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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;

import java.util.Arrays;
import java.util.Set;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

enum BuiltinColumns
{
    ID("_id", VARCHAR, true),
    SOURCE("_source", VARCHAR, false),
    SCORE("_score", REAL, false);

    public static final Set<String> NAMES = Arrays.stream(values())
            .map(BuiltinColumns::getName)
            .collect(toImmutableSet());

    private final String name;
    private final Type type;
    private final boolean supportsPredicates;

    BuiltinColumns(String name, Type type, boolean supportsPredicates)
    {
        this.name = name;
        this.type = type;
        this.supportsPredicates = supportsPredicates;
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    public ColumnMetadata getMetadata()
    {
        return new ColumnMetadata(name, type, "", null, true);
    }

    public ColumnHandle getColumnHandle()
    {
        return new ElasticsearchColumnHandle(name, type, supportsPredicates);
    }
}
