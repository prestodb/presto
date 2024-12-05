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

public class MapColumnChecksum
        extends StructureColumnChecksum
{
    private final Object checksum;
    private final Object keysChecksum;
    private final Object valuesChecksum;
    private final Optional<FloatingPointColumnChecksum> keysFloatingPointChecksum;
    private final Optional<FloatingPointColumnChecksum> valuesFloatingPointChecksum;
    private final Object cardinalityChecksum;
    private final long cardinalitySum;

    public MapColumnChecksum(
            @Nullable Object checksum,
            @Nullable Object keysChecksum,
            @Nullable Object valuesChecksum,
            Optional<FloatingPointColumnChecksum> keysFloatingPointChecksum,
            Optional<FloatingPointColumnChecksum> valuesFloatingPointChecksum,
            @Nullable Object cardinalityChecksum,
            long cardinalitySum)
    {
        this.checksum = checksum;
        this.keysChecksum = keysChecksum;
        this.valuesChecksum = valuesChecksum;
        this.keysFloatingPointChecksum = keysFloatingPointChecksum;
        this.valuesFloatingPointChecksum = valuesFloatingPointChecksum;
        this.cardinalityChecksum = cardinalityChecksum;
        this.cardinalitySum = cardinalitySum;
    }

    @Nullable
    public Object getChecksum()
    {
        return checksum;
    }

    @Nullable
    public Object getKeysChecksum()
    {
        return keysChecksum;
    }

    @Nullable
    public Object getValuesChecksum()
    {
        return valuesChecksum;
    }

    public FloatingPointColumnChecksum getKeysFloatingPointChecksum()
    {
        checkArgument(keysFloatingPointChecksum.isPresent(), "Expect Floating Point Checksum to be present, but it is not");
        return keysFloatingPointChecksum.get();
    }

    public FloatingPointColumnChecksum getValuesFloatingPointChecksum()
    {
        checkArgument(valuesFloatingPointChecksum.isPresent(), "Expect Floating Point Checksum to be present, but it is not");
        return valuesFloatingPointChecksum.get();
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

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MapColumnChecksum o = (MapColumnChecksum) obj;
        return Objects.equals(checksum, o.checksum) &&
                Objects.equals(keysChecksum, o.keysChecksum) &&
                Objects.equals(valuesChecksum, o.valuesChecksum) &&
                Objects.equals(keysFloatingPointChecksum, o.keysFloatingPointChecksum) &&
                Objects.equals(valuesFloatingPointChecksum, o.valuesFloatingPointChecksum) &&
                Objects.equals(cardinalityChecksum, o.cardinalityChecksum) &&
                Objects.equals(cardinalitySum, o.cardinalitySum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(checksum, keysChecksum, valuesChecksum, keysFloatingPointChecksum, valuesFloatingPointChecksum, cardinalityChecksum, cardinalitySum);
    }

    @Override
    public String toString()
    {
        String result = format("checksum: %s, cardinality_checksum: %s, cardinality_sum: %s", checksum, cardinalityChecksum, cardinalitySum);
        result += keysFloatingPointChecksum.isPresent() ? "" : format(", keys_checksum: %s", keysChecksum);
        result += valuesFloatingPointChecksum.isPresent() ? "" : format(", values_checksum: %s", valuesChecksum);
        result += keysFloatingPointChecksum.isPresent() ? format(". [keys] %s", keysFloatingPointChecksum.get()) : "";
        result += valuesFloatingPointChecksum.isPresent() ? format(". [values] %s", valuesFloatingPointChecksum.get()) : "";
        return result;
    }
}
