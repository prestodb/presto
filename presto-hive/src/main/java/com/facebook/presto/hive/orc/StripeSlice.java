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
package com.facebook.presto.hive.orc;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkNotNull;

public final class StripeSlice
{
    private final Slice slice;
    private final long stripeOffset;

    public StripeSlice(Slice slice, long stripeOffset)
    {
        this.slice = checkNotNull(slice, "slice is null");
        this.stripeOffset = stripeOffset;
    }

    public Slice slice(long offset, int length)
    {
        return slice.slice(Ints.checkedCast(offset - stripeOffset), length);
    }

    public boolean containsRange(long offset, long length)
    {
        return stripeOffset <= offset && offset + length <= stripeOffset + slice.length();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("slice", slice)
                .add("stripeOffset", stripeOffset)
                .toString();
    }
}
