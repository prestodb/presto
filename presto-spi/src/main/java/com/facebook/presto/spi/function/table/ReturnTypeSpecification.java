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

/**
 * The return type declaration refers to the proper columns of the table function.
 * These are the columns produced by the table function as opposed to the columns
 * of input relations passed through by the table function.
 *
 * The return type can be fixed and known at declaration time (DescribedTable),
 * dynamically determined at analysis time (GenericTable), or simply passed through
 * from input tables without adding new columns (OnlyPassThrough).
 */
public abstract class ReturnTypeSpecification
{
    /**
     * The proper columns of the table function are not known at function declaration time.
     * They must be determined at query analysis time based on the actual call arguments.
     */
    public static class GenericTable
            extends ReturnTypeSpecification
    {
        public static final GenericTable GENERIC_TABLE = new GenericTable();

        private GenericTable() {}
    }

    /**
     * The table function has no proper columns.
     */
    public static class OnlyPassThrough
            extends ReturnTypeSpecification
    {
        public static final OnlyPassThrough ONLY_PASS_THROUGH = new OnlyPassThrough();

        private OnlyPassThrough() {}
    }

    /**
     * The proper columns of the table function are known at function declaration time.
     * They do not depend on the actual call arguments.
     */
    public static class DescribedTable
            extends ReturnTypeSpecification
    {
        private final Descriptor descriptor;

        public DescribedTable(Descriptor descriptor)
        {
            requireNonNull(descriptor, "descriptor is null");
            checkArgument(descriptor.isTyped(), "field types not specified");
            this.descriptor = descriptor;
        }

        public Descriptor getDescriptor()
        {
            return descriptor;
        }
    }
}
