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
package com.facebook.presto.metadata;

import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InternalNodeSupply
{
    private final OptionalLong generalPoolCapacityInBytes;
    private final OptionalInt numberOfCpuCores;

    public InternalNodeSupply(OptionalLong generalPoolCapacityInBytes, OptionalInt numberOfCpuCores)
    {
        this.generalPoolCapacityInBytes = requireNonNull(generalPoolCapacityInBytes, "generalPoolCapacityInBytes is null");
        this.numberOfCpuCores = requireNonNull(numberOfCpuCores, "numberOfCpuCores is null");
    }

    public OptionalLong getGeneralPoolCapacityInBytes()
    {
        return generalPoolCapacityInBytes;
    }

    public OptionalInt getNumberOfCpuCores()
    {
        return numberOfCpuCores;
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
        InternalNodeSupply that = (InternalNodeSupply) o;
        return generalPoolCapacityInBytes.equals(that.generalPoolCapacityInBytes) &&
                numberOfCpuCores.equals(that.numberOfCpuCores);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(generalPoolCapacityInBytes, numberOfCpuCores);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("generalPoolCapacityInBytes", generalPoolCapacityInBytes)
                .add("numberOfCpuCores", numberOfCpuCores)
                .toString();
    }
}
