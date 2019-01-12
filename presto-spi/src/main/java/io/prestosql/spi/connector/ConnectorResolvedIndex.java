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
package io.prestosql.spi.connector;

import io.prestosql.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;

public class ConnectorResolvedIndex
{
    private final ConnectorIndexHandle indexHandle;
    private final TupleDomain<ColumnHandle> unresolvedTupleDomain;

    public ConnectorResolvedIndex(ConnectorIndexHandle indexHandle, TupleDomain<ColumnHandle> unresolvedTupleDomain)
    {
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.unresolvedTupleDomain = requireNonNull(unresolvedTupleDomain, "unresolvedTupleDomain is null");
    }

    public ConnectorIndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    public TupleDomain<ColumnHandle> getUnresolvedTupleDomain()
    {
        return unresolvedTupleDomain;
    }
}
