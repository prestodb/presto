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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class stores analysis which is aware of AccessControl
 */
public class AccessControlAwareAnalysis
{
    private final Analysis analysis;
    private final AnalysisContext analysisContext;

    // a map of users to the columns per table that they access
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> tableColumnReferences = new LinkedHashMap<>();
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> utilizedTableColumnReferences = new LinkedHashMap<>();
    private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnAndSubfieldReferences = new LinkedHashMap<>();

    public AccessControlAwareAnalysis(@Nullable AnalysisContext analysisContext, @Nullable Statement root, Map<NodeRef<Parameter>, Expression> parameters, boolean isDescribe)
    {
        this.analysis = new Analysis(root, parameters, isDescribe);
        this.analysisContext = analysisContext;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public void addTableColumnAndSubfieldReferences(AccessControl accessControl, Identity identity, Multimap<QualifiedObjectName, Subfield> tableColumnMap)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity);
        Map<QualifiedObjectName, Set<String>> columnReferences = tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
        tableColumnMap.asMap()
                .forEach((key, value) -> columnReferences.computeIfAbsent(key, k -> new HashSet<>()).addAll(value.stream().map(Subfield::getRootName).collect(toImmutableSet())));

        Map<QualifiedObjectName, Set<Subfield>> columnAndSubfieldReferences = tableColumnAndSubfieldReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
        tableColumnMap.asMap()
                .forEach((key, value) -> columnAndSubfieldReferences.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
    }

    public void addEmptyColumnReferencesForTable(AccessControl accessControl, Identity identity, QualifiedObjectName table)
    {
        AccessControlInfo accessControlInfo = new AccessControlInfo(accessControl, identity);
        tableColumnReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>()).computeIfAbsent(table, k -> new HashSet<>());
        tableColumnAndSubfieldReferences.computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>()).computeIfAbsent(table, k -> new HashSet<>());
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    public void addUtilizedTableColumnReferences(AccessControlInfo accessControlInfo, Map<QualifiedObjectName, Set<String>> utilizedTableColumns)
    {
        utilizedTableColumnReferences.put(accessControlInfo, utilizedTableColumns);
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getUtilizedTableColumnReferences()
    {
        return ImmutableMap.copyOf(utilizedTableColumnReferences);
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> getTableColumnAndSubfieldReferencesForAccessControl()
    {
        Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> references;
        if (!analysisContext.isCheckAccessControlWithSubfields()) {
            references = (analysisContext.isCheckAccessControlOnUtilizedColumnsOnly() ? utilizedTableColumnReferences : tableColumnReferences).entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey,
                            accessControlEntry -> accessControlEntry.getValue().entrySet().stream().collect(toImmutableMap(
                                    Map.Entry::getKey,
                                    tableEntry -> tableEntry.getValue().stream().map(column -> new Subfield(column, ImmutableList.of())).collect(toImmutableSet())))));
        }
        else if (!analysisContext.isCheckAccessControlOnUtilizedColumnsOnly()) {
            references = tableColumnAndSubfieldReferences;
        }
        else {
            // TODO: Properly support utilized column check. Currently, we prune whole columns, if they are not utilized.
            // We need to generalize it and exclude unutilized subfield references independently.
            references = tableColumnAndSubfieldReferences.entrySet().stream()
                    .collect(toImmutableMap(
                            Map.Entry::getKey, accessControlEntry ->
                                    accessControlEntry.getValue().entrySet().stream().collect(toImmutableMap(
                                            Map.Entry::getKey, tableEntry -> tableEntry.getValue().stream().filter(
                                                    column -> {
                                                        Map<QualifiedObjectName, Set<String>> utilizedTableReferences = utilizedTableColumnReferences.get(accessControlEntry.getKey());
                                                        if (utilizedTableReferences == null) {
                                                            return false;
                                                        }
                                                        Set<String> utilizedColumns = utilizedTableReferences.get(tableEntry.getKey());
                                                        return utilizedColumns != null && utilizedColumns.contains(column.getRootName());
                                                    })
                                                    .collect(toImmutableSet())))));
        }
        return buildMaterializedViewAccessControl(references);
    }

    /**
     * For a query on materialized view, only check the actual required access controls for its base tables. For the materialized view,
     * will not check access control by replacing with AllowAllAccessControl.
     **/
    private Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> buildMaterializedViewAccessControl(Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnReferences)
    {
        if (!(analysis.getStatement() instanceof Query) || analysis.getMaterializedViews().isEmpty()) {
            return tableColumnReferences;
        }

        Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> newTableColumnReferences = new LinkedHashMap<>();

        tableColumnReferences.forEach((accessControlInfo, references) -> {
            AccessControlInfo allowAllAccessControlInfo = new AccessControlInfo(new AllowAllAccessControl(), accessControlInfo.getIdentity());
            Map<QualifiedObjectName, Set<Subfield>> newAllowAllReferences = newTableColumnReferences.getOrDefault(allowAllAccessControlInfo, new LinkedHashMap<>());

            Map<QualifiedObjectName, Set<Subfield>> newOtherReferences = new LinkedHashMap<>();

            references.forEach((table, columns) -> {
                if (analysis.getMaterializedViews().containsKey(table)) {
                    newAllowAllReferences.computeIfAbsent(table, key -> new HashSet<>()).addAll(columns);
                }
                else {
                    newOtherReferences.put(table, columns);
                }
            });
            if (!newAllowAllReferences.isEmpty()) {
                newTableColumnReferences.put(allowAllAccessControlInfo, newAllowAllReferences);
            }
            if (!newOtherReferences.isEmpty()) {
                newTableColumnReferences.put(accessControlInfo, newOtherReferences);
            }
        });

        return newTableColumnReferences;
    }

    public static final class AccessControlInfo
    {
        private final AccessControl accessControl;
        private final Identity identity;

        public AccessControlInfo(AccessControl accessControl, Identity identity)
        {
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.identity = requireNonNull(identity, "identity is null");
        }

        public AccessControl getAccessControl()
        {
            return accessControl;
        }

        public Identity getIdentity()
        {
            return identity;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AccessControlInfo that = (AccessControlInfo) o;
            return Objects.equals(accessControl, that.accessControl) &&
                    Objects.equals(identity, that.identity);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(accessControl, identity);
        }

        @Override
        public String toString()
        {
            return format("AccessControl: %s, Identity: %s", accessControl.getClass(), identity);
        }
    }
}
