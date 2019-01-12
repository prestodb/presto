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
package io.prestosql.connector.system;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.toOptional;
import static java.util.Objects.requireNonNull;

public class DelegatingSystemTablesProvider
        implements SystemTablesProvider
{
    private final List<SystemTablesProvider> delegates;

    public DelegatingSystemTablesProvider(SystemTablesProvider... delegates)
    {
        this(ImmutableList.copyOf(delegates));
    }

    public DelegatingSystemTablesProvider(List<SystemTablesProvider> delegates)
    {
        requireNonNull(delegates, "delegates is null");
        checkArgument(delegates.size() >= 1, "empty delegates");
        this.delegates = ImmutableList.copyOf(delegates);
    }

    @Override
    public Set<SystemTable> listSystemTables(ConnectorSession session)
    {
        return delegates.stream()
                .flatMap(delegate -> delegate.listSystemTables(session).stream())
                .collect(toImmutableSet());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegates.stream()
                .map(delegate -> delegate.getSystemTable(session, tableName))
                .filter(Optional::isPresent)
                // this ensures there is 0 or 1 element in stream
                .map(Optional::get)
                .collect(toOptional());
    }
}
