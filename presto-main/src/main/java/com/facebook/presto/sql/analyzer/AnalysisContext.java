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

import com.facebook.presto.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AnalysisContext
{
    private final Optional<AnalysisContext> parent;
    private final RelationType parentRelationType;
    private final Map<String, WithQuery> namedQueries = new HashMap<>();
    private RelationType lateralTupleDescriptor = new RelationType();
    private boolean approximate;

    public AnalysisContext(AnalysisContext parent, RelationType parentRelationType)
    {
        this.parent = Optional.of(parent);
        this.parentRelationType = requireNonNull(parentRelationType, "parentRelationType is null");
        this.approximate = parent.approximate;
    }

    public AnalysisContext()
    {
        parent = Optional.empty();
        parentRelationType = new RelationType();
    }

    public void setLateralTupleDescriptor(RelationType lateralTupleDescriptor)
    {
        this.lateralTupleDescriptor = lateralTupleDescriptor;
    }

    public RelationType getLateralTupleDescriptor()
    {
        return lateralTupleDescriptor;
    }

    public boolean isApproximate()
    {
        return approximate;
    }

    public void setApproximate(boolean approximate)
    {
        this.approximate = approximate;
    }

    public void addNamedQuery(String name, WithQuery withQuery)
    {
        checkState(!namedQueries.containsKey(name), "Named query already registered: %s", name);
        namedQueries.put(name, withQuery);
    }

    public WithQuery getNamedQuery(String name)
    {
        WithQuery result = namedQueries.get(name);

        if (result == null && parent.isPresent()) {
            return parent.get().getNamedQuery(name);
        }

        return result;
    }

    public boolean isNamedQueryDeclared(String name)
    {
        return namedQueries.containsKey(name);
    }

    public RelationType getParentRelationType()
    {
        return parentRelationType;
    }

    public Optional<AnalysisContext> getParent()
    {
        return parent;
    }
}
