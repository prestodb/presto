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

package com.facebook.presto.util;

import static com.google.common.base.Preconditions.checkArgument;

public class LongValueCompressor
{
    private int leftBits = 64;
    private long value = 0;

    public LongValueCompressor store(long value, int maxBits)
    {
        checkIfCanBeStored(value, maxBits);
        this.value |= value << (leftBits - maxBits);
        leftBits -= maxBits;
        return this;
    }

    private void checkIfCanBeStored(long value, int maxBits)
    {
        checkArgument(maxBits > 0 && maxBits < 64, "Number of bits '%s' must be between 0 and 63", value);
        checkArgument(value >= 0, "Value '%s' must be greater or equal to 0", value);
        checkArgument(maxBits <= leftBits, "Not enough bits left '%s' to fit '%s' more bits", leftBits, maxBits);

        long maxValue = (1L << maxBits) - 1;

        checkArgument(
                value <= maxValue,
                "Given value '%s' does not fit into '%s' bits (max value: '%s')",
                value,
                maxBits,
                maxValue);
    }

    public long build(long value)
    {
        checkIfCanBeStored(value, Math.min(leftBits, 63)); // max 63 - no support for negative numbers
        return this.value | value;
    }
}
