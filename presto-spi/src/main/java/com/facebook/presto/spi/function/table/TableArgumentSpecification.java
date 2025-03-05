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

public class TableArgumentSpecification
        extends ArgumentSpecification
{
    private final boolean rowSemantics;
    private final boolean pruneWhenEmpty;
    private final boolean passThroughColumns;

    public TableArgumentSpecification(String name, boolean rowSemantics, boolean pruneWhenEmpty, boolean passThroughColumns)
    {
        super(name, true, null);

        this.rowSemantics = rowSemantics;
        this.pruneWhenEmpty = pruneWhenEmpty;
        this.passThroughColumns = passThroughColumns;
    }

    public TableArgumentSpecification(String name)
    {
        // defaults
        this(name, false, false, false);
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
}
