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

/**
 * The proper columns of the table function are known at function declaration time.
 * They do not depend on the actual call arguments.
 */
public class DescribedTableReturnTypeSpecification
        extends ReturnTypeSpecification
{
    private final Descriptor descriptor;
    private static final String returnType = "DESCRIBED";

    @JsonCreator
    public DescribedTableReturnTypeSpecification(@JsonProperty("descriptor") Descriptor descriptor)
    {
        requireNonNull(descriptor, "descriptor is null");
        checkArgument(descriptor.isTyped(), "field types not specified");
        this.descriptor = descriptor;
    }

    public Descriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    public String getReturnType()
    {
        return returnType;
    }
}
