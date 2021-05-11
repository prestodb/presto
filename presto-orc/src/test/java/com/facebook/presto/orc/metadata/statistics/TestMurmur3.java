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
package com.facebook.presto.orc.metadata.statistics;

import org.apache.hive.common.util.Murmur3;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestMurmur3
{
    private static final int SEED = 123;

    @Test
    public void testHashCodeM3_64()
    {
        byte[] origin = ("It was the best of times, it was the worst of times," +
                " it was the age of wisdom, it was the age of foolishness," +
                " it was the epoch of belief, it was the epoch of incredulity," +
                " it was the season of Light, it was the season of Darkness," +
                " it was the spring of hope, it was the winter of despair," +
                " we had everything before us, we had nothing before us," +
                " we were all going direct to Heaven," +
                " we were all going direct the other way.").getBytes(StandardCharsets.UTF_8);
        long hash = BloomFilter.Murmur3.hash64(origin, 0, origin.length);
        assertEquals(305830725663368540L, hash);
        assertEquals(hash, Murmur3.hash64(origin, 0, origin.length));

        byte[] originOffset = new byte[origin.length + 150];
        Arrays.fill(originOffset, (byte) 123);
        System.arraycopy(origin, 0, originOffset, 150, origin.length);
        hash = BloomFilter.Murmur3.hash64(originOffset, 150, origin.length);
        assertEquals(305830725663368540L, hash);
        assertEquals(hash, Murmur3.hash64(originOffset, 150, origin.length));
    }

    @Test
    public void test64()
    {
        ByteBuffer shortBuffer = ByteBuffer.allocate(Short.BYTES);
        ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        Random rdm = new Random(SEED);
        for (int i = 0; i < 1_000_000; ++i) {
            long ln = rdm.nextLong();
            int in = rdm.nextInt();
            short sn = (short) (rdm.nextInt(2 * Short.MAX_VALUE - 1) - Short.MAX_VALUE);
            float fn = rdm.nextFloat();
            double dn = rdm.nextDouble();
            shortBuffer.putShort(0, sn);
            assertEquals(Murmur3.hash64(shortBuffer.array()), BloomFilter.Murmur3.hash64(shortBuffer.array()));
            intBuffer.putInt(0, in);
            assertEquals(Murmur3.hash64(intBuffer.array()), BloomFilter.Murmur3.hash64(intBuffer.array()));
            longBuffer.putLong(0, ln);
            assertEquals(Murmur3.hash64(longBuffer.array()), BloomFilter.Murmur3.hash64(longBuffer.array()));
            intBuffer.putFloat(0, fn);
            assertEquals(Murmur3.hash64(intBuffer.array()), BloomFilter.Murmur3.hash64(intBuffer.array()));
            longBuffer.putDouble(0, dn);
            assertEquals(Murmur3.hash64(longBuffer.array()), BloomFilter.Murmur3.hash64(longBuffer.array()));
        }
    }
}
