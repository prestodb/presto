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

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class represents the descriptor argument passed to a Table Function.
 */
public class DescriptorArgument
        extends Argument
{
    public static final DescriptorArgument NULL_DESCRIPTOR = builder().build();
    private final Optional<Descriptor> descriptor;

    @JsonCreator
    public DescriptorArgument(@JsonProperty("descriptor") Optional<Descriptor> descriptor)
    {
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        descriptor.ifPresent(descriptorValue -> checkArgument(
                descriptorValue.getFields().stream().allMatch(field -> field.getName().isPresent()),
                "All fields of a descriptor argument must have names"));
    }

    @JsonProperty
    public Optional<Descriptor> getDescriptor()
    {
        return descriptor;
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
        DescriptorArgument that = (DescriptorArgument) o;
        return descriptor.equals(that.descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Descriptor descriptor;

        private Builder() {}

        public Builder descriptor(Descriptor descriptor)
        {
            this.descriptor = descriptor;
            return this;
        }

        public DescriptorArgument build()
        {
            return new DescriptorArgument(Optional.ofNullable(descriptor));
        }
    }
}
