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

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.security.AccessControl;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

public final class MetadataListing
{
    private MetadataListing() {}

    public static SortedMap<String, ConnectorId> listCatalogs(Session session, Metadata metadata, AccessControl accessControl)
    {
        Map<String, ConnectorId> catalogNames = metadata.getCatalogNames(session);
        Set<String> allowedCatalogs = accessControl.filterCatalogs(session.getIdentity(), catalogNames.keySet());

        ImmutableSortedMap.Builder<String, ConnectorId> result = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<String, ConnectorId> entry : catalogNames.entrySet()) {
            if (allowedCatalogs.contains(entry.getKey())) {
                result.put(entry);
            }
        }
        return result.build();
    }

    public static SortedSet<String> listSchemas(Session session, Metadata metadata, AccessControl accessControl, String catalogName)
    {
        Set<String> schemaNames = ImmutableSet.copyOf(metadata.listSchemaNames(session, catalogName));
        return ImmutableSortedSet.copyOf(accessControl.filterSchemas(session.getRequiredTransactionId(), session.getIdentity(), catalogName, schemaNames));
    }
}
