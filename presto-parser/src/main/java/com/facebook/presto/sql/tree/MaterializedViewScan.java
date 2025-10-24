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

public class MaterializedViewScan
        extends QueryBody
{
    private final QualifiedName materializedViewName;
    private final Table dataTable;
    private final Query viewQuery;

    public MaterializedViewScan(QualifiedName materializedViewName, Table dataTable, Query viewQuery)
    {
        this(Optional.empty(), materializedViewName, dataTable, viewQuery);
    }

    public MaterializedViewScan(NodeLocation location, QualifiedName materializedViewName, Table dataTable, Query viewQuery)
    {
        this(Optional.of(location), materializedViewName, dataTable, viewQuery);
    }

    private MaterializedViewScan(Optional<NodeLocation> location, QualifiedName materializedViewName, Table dataTable, Query viewQuery)
    {
        super(location);
        this.materializedViewName = requireNonNull(materializedViewName, "materializedViewName is null");
        this.dataTable = requireNonNull(dataTable, "dataTable is null");
        this.viewQuery = requireNonNull(viewQuery, "viewQuery is null");
    }

    public QualifiedName getMaterializedViewName()
    {
        return materializedViewName;
    }

    public Table getDataTable()
    {
        return dataTable;
    }

    public Query getViewQuery()
    {
        return viewQuery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMaterializedViewScan(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(dataTable);
        nodes.add(viewQuery);
        return nodes.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("materializedViewName", materializedViewName)
                .add("dataTable", dataTable)
                .add("viewQuery", viewQuery)
                .toString();
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

        MaterializedViewScan that = (MaterializedViewScan) o;
        return Objects.equals(materializedViewName, that.materializedViewName) &&
                Objects.equals(dataTable, that.dataTable) &&
                Objects.equals(viewQuery, that.viewQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(materializedViewName, dataTable, viewQuery);
    }
}
