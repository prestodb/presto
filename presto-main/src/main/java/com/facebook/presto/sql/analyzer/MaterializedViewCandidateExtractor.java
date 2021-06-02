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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static java.util.Objects.requireNonNull;

public class MaterializedViewCandidateExtractor<R, C>
        extends DefaultTraversalVisitor<R, C>
{
    private final Session session;

    private final Set<QualifiedName> tableNames = new HashSet<>();
    private final Map<QualifiedName, List<QualifiedName>> baseTableToMaterializedViews;

    public MaterializedViewCandidateExtractor(Session session, Map<QualifiedName, List<QualifiedName>> baseTableToMaterializedViews)
    {
        this.session = requireNonNull(session, "session is null");
        this.baseTableToMaterializedViews = requireNonNull(baseTableToMaterializedViews, "baseTableToMaterializedViews is null");
    }

    @Override
    protected R visitTable(Table node, C context)
    {
        tableNames.add(node.getName());
        return null;
    }

    public Optional<QualifiedObjectName> getMaterializedViewCandidate()
    {
        Set<QualifiedName> materializedViewCandidates = new HashSet();

        for (QualifiedName baseTable : tableNames) {
            List<QualifiedName> materializedViews = baseTableToMaterializedViews.getOrDefault(baseTable, new ArrayList<>());

            if (materializedViewCandidates.isEmpty()) {
                materializedViewCandidates.addAll(materializedViews);
            }
            else {
                materializedViewCandidates.retainAll(materializedViews);
            }

            if (materializedViewCandidates.isEmpty()) {
                return Optional.empty();
            }
        }

        // Temporarily it returns the first materialized view
        // TODO: Find the appropriate materialized view for the base query
        Table materializedViewTable = new Table(materializedViewCandidates.iterator().next());
        QualifiedObjectName materializedViewQualifiedObjectName = createQualifiedObjectName(session, materializedViewTable, materializedViewTable.getName());
        return Optional.of(materializedViewQualifiedObjectName);
    }
}
