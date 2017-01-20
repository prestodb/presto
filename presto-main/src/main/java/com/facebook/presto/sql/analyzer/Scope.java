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

import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.SemanticExceptions.ambiguousAttributeException;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.missingAttributeException;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

@Immutable
public class Scope
{
    private final Optional<Scope> parent;
    private final RelationType relation;
    private final Map<String, WithQuery> namedQueries;

    public static Scope create()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private Scope(
            Optional<Scope> parent,
            RelationType relation,
            Map<String, WithQuery> namedQueries)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.relation = requireNonNull(relation, "relation is null");
        this.namedQueries = ImmutableMap.copyOf(requireNonNull(namedQueries, "namedQueries is null"));
    }

    public RelationType getRelationType()
    {
        return relation;
    }

    public ResolvedField resolveField(Expression expression, QualifiedName name)
    {
        return tryResolveField(expression, name).orElseThrow(() -> missingAttributeException(expression, name));
    }

    public Optional<ResolvedField> tryResolveField(Expression expression)
    {
        QualifiedName qualifiedName = asQualifiedName(expression);
        if (qualifiedName != null) {
            return tryResolveField(expression, qualifiedName);
        }
        return Optional.empty();
    }

    private static QualifiedName asQualifiedName(Expression expression)
    {
        QualifiedName name = null;
        if (expression instanceof Identifier) {
            name = QualifiedName.of(((Identifier) expression).getName());
        }
        else if (expression instanceof DereferenceExpression) {
            name = DereferenceExpression.getQualifiedName((DereferenceExpression) expression);
        }
        return name;
    }

    public Optional<ResolvedField> tryResolveField(Expression node, QualifiedName name)
    {
        return resolveField(node, name, true);
    }

    private Optional<ResolvedField> resolveField(Expression node, QualifiedName name, boolean local)
    {
        List<Field> matches = relation.resolveFields(name);
        if (matches.size() > 1) {
            throw ambiguousAttributeException(node, name);
        }
        else if (matches.size() == 1) {
            return Optional.of(asResolvedField(getOnlyElement(matches), local));
        }
        else {
            if (isColumnReference(name, relation)) {
                return Optional.empty();
            }
            if (parent.isPresent()) {
                return parent.get().resolveField(node, name, false);
            }
            return Optional.empty();
        }
    }

    private ResolvedField asResolvedField(Field field, boolean local)
    {
        int fieldIndex = relation.indexOf(field);
        return new ResolvedField(this, field, fieldIndex, local);
    }

    public boolean isColumnReference(QualifiedName name)
    {
        Scope current = this;
        while (current != null) {
            if (isColumnReference(name, current.relation)) {
                return true;
            }
            current = current.parent.orElse(null);
        }

        return false;
    }

    private static boolean isColumnReference(QualifiedName name, RelationType relation)
    {
        while (name.getPrefix().isPresent()) {
            name = name.getPrefix().get();
            if (!relation.resolveFields(name).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Optional<WithQuery> getNamedQuery(String name)
    {
        if (namedQueries.containsKey(name)) {
            return Optional.of(namedQueries.get(name));
        }

        if (parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return Optional.empty();
    }

    public static final class Builder
    {
        private RelationType relationType = new RelationType();
        private Optional<Boolean> approximate = Optional.empty();
        private final Map<String, WithQuery> namedQueries = new HashMap<>();
        private Optional<Scope> parent = Optional.empty();

        public Builder withRelationType(RelationType relationType)
        {
            this.relationType = requireNonNull(relationType, "relationType is null");
            return this;
        }

        public Builder withParent(Scope parent)
        {
            this.parent = Optional.of(parent);
            return this;
        }

        public Builder withNamedQuery(String name, WithQuery withQuery)
        {
            checkArgument(!containsNamedQuery(name), "Query '%s' is already added", name);
            namedQueries.put(name, withQuery);
            return this;
        }

        public boolean containsNamedQuery(String name)
        {
            return namedQueries.containsKey(name);
        }

        public Scope build()
        {
            return new Scope(parent, relationType, namedQueries);
        }
    }
}
