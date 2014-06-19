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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateTable
        extends Statement
{
    private final QualifiedName name;
    private final List<TableElement> tableElementList;
    private final List<TableElement> partitionElementList;
    private final Optional<Statement> ifNotExists;
    private final Optional<String> serde;
    private final Optional<String> inputFormat;
    private final Optional<String> outputFormat;
    private final Optional<String> location;

    public CreateTable(QualifiedName name,
                        Optional<Statement> ifNotExists,
                        List<TableElement> tableElementList,
                        List<TableElement> partitionElementList,
                        Optional<String> serde,
                        Optional<String> inputFormat,
                        Optional<String> outputFormat,
                        Optional<String> location)
    {
        this.name = checkNotNull(name, "name is null");
        if (tableElementList == null) {
            this.tableElementList = ImmutableList.of();
        }
        else {
            this.tableElementList = ImmutableList.copyOf(tableElementList);
        }
        if (partitionElementList == null) {
            this.partitionElementList = ImmutableList.of();
        }
        else {
            this.partitionElementList = ImmutableList.copyOf(partitionElementList);
        }
        this.ifNotExists = ifNotExists;
        this.serde = serde;
        this.inputFormat = inputFormat;
        this.outputFormat = outputFormat;
        this.location = location;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<TableElement> getTableElementList()
    {
        return tableElementList;
    }

    public List<TableElement> getPartitionElementList()
    {
        return partitionElementList;
    }

    public Optional<Statement> getIfNotExists()
    {
        return ifNotExists;
    }

    public Optional<String> getSerde()
    {
        return serde;
    }

    public Optional<String> getInputFormat()
    {
        return inputFormat;
    }

    public Optional<String> getOutputFormat()
    {
        return outputFormat;
    }

    public Optional<String> getLocation()
    {
        return location;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, ifNotExists, tableElementList, partitionElementList, serde, inputFormat, outputFormat, location);
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
        CreateTable o = (CreateTable) obj;
        return Objects.equal(name, o.name)
                && Objects.equal(tableElementList, o.tableElementList)
                && Objects.equal(partitionElementList, o.partitionElementList)
                && Objects.equal(ifNotExists, o.ifNotExists)
                && Objects.equal(serde, o.serde)
                && Objects.equal(inputFormat, o.inputFormat)
                && Objects.equal(outputFormat, o.outputFormat)
                && Objects.equal(location, o.location);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("tableElementList", tableElementList)
                .add("partitionElementList", partitionElementList)
                .add("ifNotExists", ifNotExists)
                .add("serde", serde)
                .add("inputFormat", inputFormat)
                .add("outputFormat", outputFormat)
                .add("location", location)
                .omitNullValues()
                .toString();
    }
}
