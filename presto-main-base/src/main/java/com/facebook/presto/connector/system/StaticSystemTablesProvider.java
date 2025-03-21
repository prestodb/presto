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
package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

public class StaticSystemTablesProvider
        implements SystemTablesProvider
{
    private final Set<SystemTable> systemTables;
    private final Map<SchemaTableName, SystemTable> systemTablesMap;

    public StaticSystemTablesProvider(Set<SystemTable> systemTables)
    {
        this.systemTables = ImmutableSet.copyOf(systemTables);
        this.systemTablesMap = systemTables.stream()
                .collect(toImmutableMap(
                        table -> table.getTableMetadata().getTable(),
                        identity()));
    }

    @Override
    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        return systemTables;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.ofNullable(systemTablesMap.get(tableName));
    }
}
