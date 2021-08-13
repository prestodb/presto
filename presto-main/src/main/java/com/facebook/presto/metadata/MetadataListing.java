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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.GrantInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public final class MetadataListing
{
    private MetadataListing() {}

    public static SortedMap<String, ConnectorId> listCatalogs(Session session, Metadata metadata, AccessControl accessControl)
    {
        Map<String, ConnectorId> catalogNames = metadata.getCatalogNames(session);
        Set<String> allowedCatalogs = accessControl.filterCatalogs(session.getIdentity(), session.getAccessControlContext(), catalogNames.keySet());

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
        return ImmutableSortedSet.copyOf(accessControl.filterSchemas(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), catalogName, schemaNames));
    }

    public static Set<SchemaTableName> listTables(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listTables(session, prefix).stream()
                .map(MetadataUtil::toSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), prefix.getCatalogName(), tableNames);
    }

    public static Set<SchemaTableName> listViews(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Set<SchemaTableName> tableNames = metadata.listViews(session, prefix).stream()
                .map(MetadataUtil::toSchemaTableName)
                .collect(toImmutableSet());
        return accessControl.filterTables(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), prefix.getCatalogName(), tableNames);
    }

    public static Set<GrantInfo> listTablePrivileges(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        List<GrantInfo> grants = metadata.listTablePrivileges(session, prefix);
        Set<SchemaTableName> allowedTables = accessControl.filterTables(
                session.getRequiredTransactionId(),
                session.getIdentity(),
                session.getAccessControlContext(),
                prefix.getCatalogName(),
                grants.stream().map(GrantInfo::getSchemaTableName).collect(toImmutableSet()));

        return grants.stream()
                .filter(grantInfo -> allowedTables.contains(grantInfo.getSchemaTableName()))
                .collect(toImmutableSet());
    }

    public static Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(Session session, Metadata metadata, AccessControl accessControl, QualifiedTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> tableColumns = metadata.listTableColumns(session, prefix).entrySet().stream()
                .collect(toImmutableMap(entry -> toSchemaTableName(entry.getKey()), Entry::getValue));
        Set<SchemaTableName> allowedTables = accessControl.filterTables(
                session.getRequiredTransactionId(),
                session.getIdentity(),
                session.getAccessControlContext(),
                prefix.getCatalogName(),
                tableColumns.keySet());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
        for (Entry<SchemaTableName, List<ColumnMetadata>> entry : tableColumns.entrySet()) {
            if (allowedTables.contains(entry.getKey())) {
                result.put(entry);
            }
        }
        return result.build();
    }
}
