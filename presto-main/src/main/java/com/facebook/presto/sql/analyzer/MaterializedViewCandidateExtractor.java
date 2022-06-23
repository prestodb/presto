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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Table;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static java.util.Objects.requireNonNull;

public class MaterializedViewCandidateExtractor
        extends DefaultTraversalVisitor<Void, Void>
{
    private final Set<QualifiedObjectName> tableNames = new HashSet<>();
    private final Metadata metadata;
    private final Session session;

    public MaterializedViewCandidateExtractor(Session session, Metadata metadata)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    protected Void visitTable(Table node, Void context)
    {
        tableNames.add(createQualifiedObjectName(session, node, node.getName()));
        return null;
    }

    public Set<QualifiedObjectName> getMaterializedViewCandidates()
    {
        Set<QualifiedObjectName> materializedViewCandidates = new HashSet<>();

        for (QualifiedObjectName baseTable : tableNames) {
            List<QualifiedObjectName> materializedViews = metadata.getReferencedMaterializedViews(session, baseTable);

            if (materializedViewCandidates.isEmpty()) {
                materializedViewCandidates.addAll(materializedViews);
            }
            else {
                materializedViewCandidates.retainAll(materializedViews);
            }

            if (materializedViewCandidates.isEmpty()) {
                return materializedViewCandidates;
            }
        }

        return materializedViewCandidates;
    }
}
