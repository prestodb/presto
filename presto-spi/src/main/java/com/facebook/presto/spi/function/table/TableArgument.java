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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.type.RowType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This class represents the table argument passed to a Table Function.
 */
public class TableArgument
        extends Argument
{
    private final RowType rowType;
    private final List<String> partitionBy;
    private final List<String> orderBy;

    @JsonCreator
    public TableArgument(
            @JsonProperty("rowType") RowType rowType,
            @JsonProperty("partitionBy") List<String> partitionBy,
            @JsonProperty("orderBy") List<String> orderBy,
            @JsonProperty("fields") List<RowType.Field> fields)
    {
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null");
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
    }

    @JsonProperty
    public RowType getRowType()
    {
        return rowType;
    }

    @JsonProperty
    public List<RowType.Field> getFields()
    {
        return rowType.getFields();
    }

    @JsonProperty
    public List<String> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public List<String> getOrderBy()
    {
        return orderBy;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private RowType rowType;
        private List<String> partitionBy = new ArrayList<>();
        private List<String> orderBy = new ArrayList<>();

        private Builder() {}

        public Builder rowType(RowType rowType)
        {
            this.rowType = rowType;
            return this;
        }

        public Builder partitionBy(String partitionBy)
        {
            this.partitionBy.add(partitionBy);
            return this;
        }

        public Builder partitionBy(List<String> partitionBy)
        {
            this.partitionBy.addAll(partitionBy);
            return this;
        }

        public Builder orderBy(String orderBy)
        {
            this.orderBy.add(orderBy);
            return this;
        }

        public Builder orderBy(List<String> orderBy)
        {
            this.orderBy.addAll(orderBy);
            return this;
        }

        public TableArgument build()
        {
            return new TableArgument(rowType, partitionBy, orderBy, rowType.getFields());
        }
    }
}
