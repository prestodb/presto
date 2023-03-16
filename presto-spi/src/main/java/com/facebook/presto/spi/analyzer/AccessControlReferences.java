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
package com.facebook.presto.spi.analyzer;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.Subfield;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

// TODO: Should we migrate existing table column references to this class?
public class AccessControlReferences
{
    private final Map<AccessControlRole, Set<AccessControlInfoForTable>> tableReferences;
    private Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnAndSubfieldReferencesForAccessControl;

    public AccessControlReferences()
    {
        tableReferences = new LinkedHashMap<>();
    }

    public Map<AccessControlRole, Set<AccessControlInfoForTable>> getTableReferences()
    {
        return unmodifiableMap(tableReferences);
    }

    public void addTableReference(AccessControlRole role, AccessControlInfoForTable accessControlInfoForTable)
    {
        tableReferences.computeIfAbsent(role, r -> new LinkedHashSet<>()).add(accessControlInfoForTable);
    }

    public Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> getTableColumnAndSubfieldReferencesForAccessControl()
    {
        return tableColumnAndSubfieldReferencesForAccessControl;
    }

    public void setTableColumnAndSubfieldReferencesForAccessControl(Map<AccessControlInfo, Map<QualifiedObjectName, Set<Subfield>>> tableColumnAndSubfieldReferencesForAccessControl)
    {
        this.tableColumnAndSubfieldReferencesForAccessControl = unmodifiableMap(requireNonNull(tableColumnAndSubfieldReferencesForAccessControl, "tableColumnAndSubfieldReferencesForAccessControl is null"));
    }
}
