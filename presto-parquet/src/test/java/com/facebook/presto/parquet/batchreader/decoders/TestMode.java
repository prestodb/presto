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
package com.facebook.presto.parquet.batchreader.decoders;

public enum TestMode
{
    UPPER_BOUND(Integer.MAX_VALUE, Long.MAX_VALUE), LOWER_BOUND(Integer.MIN_VALUE, 0), ARBITRARY(237, 237 * (1L << 31));

    private final int testInt;
    private final long testLong;

    TestMode(int i, long l)
    {
        this.testInt = i;
        this.testLong = l;
    }

    public int getInt()
    {
        return testInt;
    }

    public long getLong()
    {
        return testLong;
    }

    public int getPositiveUpperBoundedInt(int upper)
    {
        if (this.name().equals(LOWER_BOUND.name()) || upper <= 0) {
            return 0;
        }

        if (this.name().equals(UPPER_BOUND.name())) {
            return upper;
        }

        return ARBITRARY.testInt % upper;
    }
}
