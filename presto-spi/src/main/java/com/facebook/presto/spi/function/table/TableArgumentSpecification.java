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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableArgumentSpecification
        extends ArgumentSpecification
{
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;

    @JsonCreator
    public TableArgumentSpecification(
            @JsonProperty("name") String name,
            @JsonProperty("rowSemantics") boolean rowSemantics,
            @JsonProperty("pruneWhenEmpty") boolean pruneWhenEmpty,
            @JsonProperty("passThroughColumns") boolean passThroughColumns)
    {
        super(name, true, null);

        requireNonNull(pruneWhenEmpty, "The pruneWhenEmpty property is not set");
        checkArgument(!rowSemantics || pruneWhenEmpty, "Cannot set the KEEP WHEN EMPTY property for a table argument with row semantics");

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
    }

    @JsonProperty
    public boolean isRowSemantics()
    {
        return rowSemantics;
    }

    @JsonProperty
    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

    @JsonProperty
    public boolean isPassThroughColumns()
    {
        return passThroughColumns;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String name;
        private boolean rowSemantics;
        private boolean pruneWhenEmpty;
        private boolean passThroughColumns;

        private Builder() {}

        public Builder name(String name)
        {
            this.name = name;
            return this;
        }

        public Builder rowSemantics()
        {
            this.rowSemantics = true;
            this.pruneWhenEmpty = true;
            return this;
        }

        public Builder pruneWhenEmpty()
        {
            this.pruneWhenEmpty = true;
            return this;
        }

        public Builder keepWhenEmpty()
        {
            this.pruneWhenEmpty = false;
            return this;
        }

        public Builder passThroughColumns()
        {
            this.passThroughColumns = true;
            return this;
        }

        public TableArgumentSpecification build()
        {
            return new TableArgumentSpecification(name, rowSemantics, pruneWhenEmpty, passThroughColumns);
        }
    }
}
