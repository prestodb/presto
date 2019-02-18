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

public class Analyze
        extends Statement
{
    private final QualifiedName tableName;
    private final List<Property> properties;

    public Analyze(QualifiedName tableName, List<Property> properties)
    {
        this(Optional.empty(), tableName, properties);
    }

    public Analyze(NodeLocation location, QualifiedName tableName, List<Property> properties)
    {
        this(Optional.of(location), tableName, properties);
    }

    private Analyze(Optional<NodeLocation> location, QualifiedName tableName, List<Property> properties)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAnalyze(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return properties;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, properties);
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
        Analyze o = (Analyze) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("properties", properties)
                .toString();
    }
}
