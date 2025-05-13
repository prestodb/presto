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
package com.facebook.presto.verifier.checksum;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class ArrayColumnChecksum
        extends StructureColumnChecksum
{
    private final Object checksum;
    private final Object cardinalityChecksum;
    private final long cardinalitySum;
    // For array(floating point) or array(varchar) we have extra aggregations collected.
    private final Optional<FloatingPointColumnChecksum> floatingPointChecksum;

    public ArrayColumnChecksum(
            @Nullable Object checksum,
            @Nullable Object cardinalityChecksum,
            long cardinalitySum,
            Optional<FloatingPointColumnChecksum> floatingPointChecksum)
    {
        this.checksum = checksum;
        this.cardinalityChecksum = cardinalityChecksum;
        this.cardinalitySum = cardinalitySum;
        this.floatingPointChecksum = floatingPointChecksum;
    }

    @Nullable
    public Object getChecksum()
    {
        return checksum;
    }

    @Override
    @Nullable
    public Object getCardinalityChecksum()
    {
        return cardinalityChecksum;
    }

    @Override
    public long getCardinalitySum()
    {
        return cardinalitySum;
    }

    public FloatingPointColumnChecksum getFloatingPointChecksum()
    {
        checkArgument(floatingPointChecksum.isPresent(), "Expect Floating Point Checksum to be present, but it is not");
        return floatingPointChecksum.get();
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
        ArrayColumnChecksum o = (ArrayColumnChecksum) obj;
        return Objects.equals(checksum, o.checksum) &&
                Objects.equals(floatingPointChecksum, o.floatingPointChecksum) &&
                Objects.equals(cardinalityChecksum, o.cardinalityChecksum) &&
                Objects.equals(cardinalitySum, o.cardinalitySum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(checksum, floatingPointChecksum, cardinalityChecksum, cardinalitySum);
    }

    @Override
    public String toString()
    {
        if (!floatingPointChecksum.isPresent()) {
            return format("checksum: %s, cardinality_checksum: %s, cardinality_sum: %s", checksum, cardinalityChecksum, cardinalitySum);
        }
        else {
            return format(
                    "%s, cardinality_checksum: %s, cardinality_sum: %s",
                    floatingPointChecksum.get(),
                    cardinalityChecksum,
                    cardinalitySum);
        }
    }
}
