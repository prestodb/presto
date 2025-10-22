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

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TableArgumentSpecification
        extends ArgumentSpecification
{
    public static final String argumentType = "TableArgumentSpecification";
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;

    private TableArgumentSpecification(String name, boolean rowSemantics, boolean pruneWhenEmpty, boolean passThroughColumns)
    {
        super(name, true, null);

        requireNonNull(pruneWhenEmpty, "The pruneWhenEmpty property is not set");
        checkArgument(!rowSemantics || pruneWhenEmpty, "Cannot set the KEEP WHEN EMPTY property for a table argument with row semantics");

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
    }

    public boolean isRowSemantics()
    {
        return rowSemantics;
    }

    public boolean isPruneWhenEmpty()
    {
        return pruneWhenEmpty;
    }

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
        // A table argument with row semantics is processed on a row-by-row basis.
        // Partitioning or ordering is not applicable.
        private boolean rowSemantics;
        // The prune when empty property indicates that if the given table argument is empty, the function returns empty result.
        // This property is used to optimize queries involving table functions.
        // The keep when empty property indicates that the function should be executed even if the table argument is empty.
        private boolean pruneWhenEmpty;
        // If a table argument has pass-through columns, all of its columns are passed on output.
        // For a table argument without this property, only the partitioning columns are passed on output.
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

    @Override
    public String getArgumentType()
    {
        return argumentType;
    }
}
