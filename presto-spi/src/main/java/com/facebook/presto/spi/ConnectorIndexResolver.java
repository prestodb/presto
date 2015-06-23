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
package com.facebook.presto.spi;

import java.util.List;
import java.util.Set;

public interface ConnectorIndexResolver
{
    // TODO: should we allow partial index resolutions? (e.g. only index on colA when asking for an index on colA and colB)
    ConnectorResolvedIndex resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, TupleDomain<ColumnHandle> tupleDomain);

    // Filtered projection can be used if it can provide all the columns needed (outputColumns).
    default ConnectorResolvedIndex resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        return resolveIndex(session, tableHandle, indexableColumns, tupleDomain);
    }

    ConnectorIndex getIndex(ConnectorSession session, ConnectorIndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema);
}
