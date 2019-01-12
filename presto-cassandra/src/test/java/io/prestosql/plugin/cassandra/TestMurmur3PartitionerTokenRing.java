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

import org.testng.annotations.Test;

import java.math.BigInteger;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.testng.Assert.assertEquals;

public class TestMurmur3PartitionerTokenRing
{
    private static final Murmur3PartitionerTokenRing tokenRing = Murmur3PartitionerTokenRing.INSTANCE;

    @Test
    public void testGetTokenCountInRange()
    {
        assertEquals(tokenRing.getTokenCountInRange("0", "1"), ONE);
        assertEquals(tokenRing.getTokenCountInRange("-1", "1"), new BigInteger("2"));
        assertEquals(tokenRing.getTokenCountInRange("-100", "100"), new BigInteger("200"));
        assertEquals(tokenRing.getTokenCountInRange("0", "10"), new BigInteger("10"));
        assertEquals(tokenRing.getTokenCountInRange("1", "11"), new BigInteger("10"));
        assertEquals(tokenRing.getTokenCountInRange("0", "0"), ZERO);
        assertEquals(tokenRing.getTokenCountInRange("1", "1"), ZERO);
        assertEquals(tokenRing.getTokenCountInRange(Long.toString(Long.MIN_VALUE), Long.toString(Long.MIN_VALUE)), BigInteger.valueOf(2).pow(64).subtract(ONE));
        assertEquals(tokenRing.getTokenCountInRange("1", "0"), BigInteger.valueOf(2).pow(64).subtract(BigInteger.valueOf(2)));
    }

    @Test
    public void testGetRingFraction()
    {
        assertEquals(tokenRing.getRingFraction("1", "1"), 0.0, 0.001);
        assertEquals(tokenRing.getRingFraction("1", "0"), 1.0, 0.001);
        assertEquals(tokenRing.getRingFraction("0", Long.toString(Long.MAX_VALUE)), 0.5, 0.001);
        assertEquals(tokenRing.getRingFraction(Long.toString(Long.MIN_VALUE), Long.toString(Long.MAX_VALUE)), 1.0, 0.001);
        assertEquals(tokenRing.getRingFraction(Long.toString(Long.MIN_VALUE), Long.toString(Long.MIN_VALUE)), 1.0, 0.001);
    }
}
