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

public class FloatingPointColumnChecksum
        extends ColumnChecksum
{
    private final Object sum;
    private final long nanCount;
    private final long positiveInfinityCount;
    private final long negativeInfinityCount;
    private final long rowCount;

    public FloatingPointColumnChecksum(@Nullable Object sum, long nanCount, long positiveInfinityCount, long negativeInfinityCount, long rowCount)
    {
        this.sum = sum;
        this.positiveInfinityCount = positiveInfinityCount;
        this.negativeInfinityCount = negativeInfinityCount;
        this.nanCount = nanCount;
        this.rowCount = rowCount;
    }

    @Nullable
    public Object getSum()
    {
        return sum;
    }

    public long getNanCount()
    {
        return nanCount;
    }

    public long getPositiveInfinityCount()
    {
        return positiveInfinityCount;
    }

    public long getNegativeInfinityCount()
    {
        return negativeInfinityCount;
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
        FloatingPointColumnChecksum o = (FloatingPointColumnChecksum) obj;
        return Objects.equals(sum, o.sum) &&
                Objects.equals(nanCount, o.nanCount) &&
                Objects.equals(positiveInfinityCount, o.positiveInfinityCount) &&
                Objects.equals(negativeInfinityCount, o.negativeInfinityCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sum, nanCount, positiveInfinityCount, negativeInfinityCount);
    }

    @Override
    public String toString()
    {
        String mean = (rowCount > 0 && sum != null) ? ", mean: " + ((double) sum / rowCount) : "";
        return format("sum: %s, NaN: %s, +infinity: %s, -infinity: %s%s", sum, nanCount, positiveInfinityCount, negativeInfinityCount, mean);
    }
}
