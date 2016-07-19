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
package com.facebook.presto.raptor.storage.organization;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShardRange
{
    private final Tuple minTuple;
    private final Tuple maxTuple;

    public static ShardRange of(Tuple min, Tuple max)
    {
        return new ShardRange(min, max);
    }

    private ShardRange(Tuple minTuple, Tuple maxTuple)
    {
        this.minTuple = requireNonNull(minTuple, "minTuple is null");
        this.maxTuple = requireNonNull(maxTuple, "maxTuple is null");
    }

    public Tuple getMinTuple()
    {
        return minTuple;
    }

    public Tuple getMaxTuple()
    {
        return maxTuple;
    }

    public boolean encloses(ShardRange other)
    {
        return this.getMinTuple().compareTo(other.getMinTuple()) <= 0 &&
                this.getMaxTuple().compareTo(other.getMaxTuple()) >= 0;
    }

    public boolean overlaps(ShardRange other)
    {
        return this.getMinTuple().compareTo(other.getMaxTuple()) <= 0 &&
                other.getMinTuple().compareTo(this.getMaxTuple()) <= 0;
    }

    public boolean adjacent(ShardRange other)
    {
        Object o1Min = this.getMinTuple().getValues().get(0);
        Object o1Max = this.getMaxTuple().getValues().get(0);

        Object o2Min = other.getMinTuple().getValues().get(0);
        Object o2Max = other.getMaxTuple().getValues().get(0);

        return o1Max.equals(o2Min) || o2Max.equals(o1Min);
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
        ShardRange that = (ShardRange) o;
        return Objects.equals(minTuple, that.minTuple) &&
                Objects.equals(maxTuple, that.maxTuple);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minTuple, maxTuple);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("minTuple", minTuple)
                .add("maxTuple", maxTuple)
                .toString();
    }
}
