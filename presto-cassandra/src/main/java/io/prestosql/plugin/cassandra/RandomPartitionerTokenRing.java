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

import static com.google.common.base.Preconditions.checkArgument;
import static java.math.BigInteger.ZERO;

public final class RandomPartitionerTokenRing
        implements TokenRing
{
    public static final RandomPartitionerTokenRing INSTANCE = new RandomPartitionerTokenRing();

    private static final BigInteger MIN_TOKEN = BigInteger.valueOf(-1);
    private static final BigInteger MAX_TOKEN = BigInteger.valueOf(2).pow(127);
    private static final BigInteger TOTAL_TOKEN_COUNT = MAX_TOKEN.subtract(MIN_TOKEN);

    private RandomPartitionerTokenRing() {}

    @Override
    public double getRingFraction(String start, String end)
    {
        return getTokenCountInRange(start, end).doubleValue() / TOTAL_TOKEN_COUNT.doubleValue();
    }

    @Override
    public BigInteger getTokenCountInRange(String startToken, String endToken)
    {
        BigInteger start = new BigInteger(startToken);
        checkTokenBounds(start);
        BigInteger end = new BigInteger(endToken);
        checkTokenBounds(end);

        if (start.equals(end)) {
            if (start.equals(MIN_TOKEN)) {
                return TOTAL_TOKEN_COUNT;
            }
            else {
                return ZERO;
            }
        }

        BigInteger result = end.subtract(start);
        if (end.compareTo(start) <= 0) {
            result = result.add(TOTAL_TOKEN_COUNT);
        }
        return result;
    }

    private static void checkTokenBounds(BigInteger token)
    {
        checkArgument(token.compareTo(MIN_TOKEN) >= 0, "token [%s] must be greater or equal than %s", token, MIN_TOKEN);
        checkArgument(token.compareTo(MAX_TOKEN) <= 0, "token [%s] must be less or equal than %s", token, MAX_TOKEN);
    }
}
