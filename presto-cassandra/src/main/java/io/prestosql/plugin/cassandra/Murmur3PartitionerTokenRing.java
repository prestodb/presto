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
package io.prestosql.plugin.cassandra;

import java.math.BigInteger;

import static java.math.BigInteger.ZERO;

public final class Murmur3PartitionerTokenRing
        implements TokenRing
{
    public static final Murmur3PartitionerTokenRing INSTANCE = new Murmur3PartitionerTokenRing();

    private static final long MIN_TOKEN = Long.MIN_VALUE;
    private static final long MAX_TOKEN = Long.MAX_VALUE;
    private static final BigInteger TOTAL_TOKEN_COUNT = BigInteger.valueOf(MAX_TOKEN).subtract(BigInteger.valueOf(MIN_TOKEN));

    private Murmur3PartitionerTokenRing() {}

    @Override
    public double getRingFraction(String start, String end)
    {
        return getTokenCountInRange(start, end).doubleValue() / TOTAL_TOKEN_COUNT.doubleValue();
    }

    @Override
    public BigInteger getTokenCountInRange(String startToken, String endToken)
    {
        long start = Long.parseLong(startToken);
        long end = Long.parseLong(endToken);

        if (start == end) {
            if (start == MIN_TOKEN) {
                return TOTAL_TOKEN_COUNT;
            }
            else {
                return ZERO;
            }
        }

        BigInteger result = BigInteger.valueOf(end).subtract(BigInteger.valueOf(start));
        if (end <= start) {
            result = result.add(TOTAL_TOKEN_COUNT);
        }
        return result;
    }
}
