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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * A QueryBody that holds the original QuerySpecification along with all materialized view
 * rewrite candidates for cost-based selection. This node is produced during
 * query rewriting when multiple MV candidates are available, deferring the
 * selection to after logical planning where cost comparison can be performed.
 */
public class QueryWithMVRewriteCandidates
        extends QueryBody
{
    private final QuerySpecification originalQuery;
    private final List<MVRewriteCandidate> candidates;

    public QueryWithMVRewriteCandidates(
            QuerySpecification originalQuery,
            List<MVRewriteCandidate> candidates)
    {
        this(Optional.empty(), originalQuery, candidates);
    }

    public QueryWithMVRewriteCandidates(
            NodeLocation location,
            QuerySpecification originalQuery,
            List<MVRewriteCandidate> candidates)
    {
        this(Optional.of(location), originalQuery, candidates);
    }

    private QueryWithMVRewriteCandidates(
            Optional<NodeLocation> location,
            QuerySpecification originalQuery,
            List<MVRewriteCandidate> candidates)
    {
        super(location);
        this.originalQuery = requireNonNull(originalQuery, "originalQuery is null");
        this.candidates = ImmutableList.copyOf(requireNonNull(candidates, "candidates is null"));
    }

    public QuerySpecification getOriginalQuery()
    {
        return originalQuery;
    }

    public List<MVRewriteCandidate> getCandidates()
    {
        return candidates;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQueryWithMVRewriteCandidates(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(originalQuery);
        for (MVRewriteCandidate candidate : candidates) {
            nodes.add(candidate.getRewrittenQuery());
        }
        return nodes.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("originalQuery", originalQuery)
                .add("candidatesCount", candidates.size())
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QueryWithMVRewriteCandidates o = (QueryWithMVRewriteCandidates) obj;
        return Objects.equals(originalQuery, o.originalQuery) &&
                Objects.equals(candidates, o.candidates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(originalQuery, candidates);
    }

    /**
     * Represents a single materialized view rewrite candidate with its
     * rewritten query and MV metadata.
     */
    public static class MVRewriteCandidate
    {
        private final QuerySpecification rewrittenQuery;
        private final String materializedViewCatalog;
        private final String materializedViewSchema;
        private final String materializedViewName;

        public MVRewriteCandidate(
                QuerySpecification rewrittenQuery,
                String materializedViewCatalog,
                String materializedViewSchema,
                String materializedViewName)
        {
            this.rewrittenQuery = requireNonNull(rewrittenQuery, "rewrittenQuery is null");
            this.materializedViewCatalog = requireNonNull(materializedViewCatalog, "materializedViewCatalog is null");
            this.materializedViewSchema = requireNonNull(materializedViewSchema, "materializedViewSchema is null");
            this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        }

        public QuerySpecification getRewrittenQuery()
        {
            return rewrittenQuery;
        }

        public String getMaterializedViewCatalog()
        {
            return materializedViewCatalog;
        }

        public String getMaterializedViewSchema()
        {
            return materializedViewSchema;
        }

        public String getMaterializedViewName()
        {
            return materializedViewName;
        }

        public String getFullyQualifiedName()
        {
            return materializedViewCatalog + "." + materializedViewSchema + "." + materializedViewName;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            MVRewriteCandidate o = (MVRewriteCandidate) obj;
            return Objects.equals(rewrittenQuery, o.rewrittenQuery) &&
                    Objects.equals(materializedViewCatalog, o.materializedViewCatalog) &&
                    Objects.equals(materializedViewSchema, o.materializedViewSchema) &&
                    Objects.equals(materializedViewName, o.materializedViewName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(rewrittenQuery, materializedViewCatalog, materializedViewSchema, materializedViewName);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("materializedView", getFullyQualifiedName())
                    .toString();
        }
    }
}
