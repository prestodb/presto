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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class InformationSchemaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final InformationSchemaTableHandle table;
    private final Set<QualifiedTablePrefix> prefixes;

    @JsonCreator
    public InformationSchemaTableLayoutHandle(
            @JsonProperty("table") InformationSchemaTableHandle table,
            @JsonProperty("prefixes") Set<QualifiedTablePrefix> prefixes)
    {
        this.table = requireNonNull(table, "table is null");
        this.prefixes = ImmutableSet.copyOf(requireNonNull(prefixes, "prefixes is null"));
    }

    @JsonProperty
    public InformationSchemaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Set<QualifiedTablePrefix> getPrefixes()
    {
        return prefixes;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
