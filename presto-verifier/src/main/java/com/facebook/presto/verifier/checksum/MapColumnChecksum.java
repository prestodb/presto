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

import static java.lang.String.format;

public class MapColumnChecksum
        extends StructureColumnChecksum
{
    private final Object checksum;
    private final Object keysChecksum;
    private final Object valuesChecksum;
    private final long cardinalitySum;

    public MapColumnChecksum(
            @Nullable Object checksum,
            @Nullable Object keysChecksum,
            @Nullable Object valuesChecksum,
            @Nullable long cardinalitySum)
    {
        this.checksum = checksum;
        this.keysChecksum = keysChecksum;
        this.valuesChecksum = valuesChecksum;
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
                Objects.equals(cardinalitySum, o.cardinalitySum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(checksum, keysChecksum, valuesChecksum, cardinalitySum);
    }

    @Override
    public String toString()
    {
        return format("checksum: %s, keys_checksum: %s, values_checksum: %s, cardinality_sum: %s", checksum, keysChecksum, valuesChecksum, cardinalitySum);
    }
}
