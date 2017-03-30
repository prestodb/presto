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
package com.facebook.presto.hdfs.function;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;

import java.nio.charset.Charset;
import java.util.Objects;

/**
 * presto-root
 *
 * Using MurmurHash function
 * @author Jelly
 */
@ThreadSafe
public final class Function0 extends com.facebook.presto.hdfs.function.HashFunction
{
    private final int seed;
    private final HashFunction hasher;

    public Function0(int seed)
    {
        this.seed = seed;
        hasher = Hashing.murmur3_128(seed);
    }

    public Function0()
    {
        this.seed = 1318007700;
        hasher = Hashing.murmur3_128(seed);
    }

    @Override
    public long apply(String v)
    {
        return hasher.hashString(v.subSequence(0, v.length()), Charset.defaultCharset()).asLong();
    }

    @Override
    public long apply(int v)
    {
        return hasher.hashInt(v).asLong();
    }

    @Override
    public long apply(long v)
    {
        return hasher.hashLong(v).asLong();
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Function0 other = (Function0) obj;
        return Objects.equals(hashCode(), other.hashCode());
    }

    @Override
    public String toString()
    {
        return "function0";
    }
}
