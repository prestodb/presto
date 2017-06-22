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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import javax.annotation.concurrent.ThreadSafe;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * presto-root
 *
 * Using MurmurHash function
 * @author Jelly
 */
@ThreadSafe
@JsonTypeName(value = "function0")
public final class Function0 implements Function
{
    private final int seed;
    private final int fiberNum;
    private final HashFunction hasher;

    @JsonCreator
    public Function0(@JsonProperty("seed") int seed,
                     @JsonProperty("fiberNum") int fiberNum)
    {
        this.seed = seed;
        this.fiberNum = fiberNum;
        this.hasher = Hashing.murmur3_128(seed);
    }

    public Function0(int fiberNum)
    {
        this(1318007700, fiberNum);
    }

    @JsonProperty
    public int getSeed()
    {
        return seed;
    }

    @Override
    public long apply(String v)
    {
        long k = hasher.hashString(v.subSequence(0, v.length()), StandardCharsets.UTF_8).asLong();
        return ((k % fiberNum) + fiberNum) % fiberNum;
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
        return Objects.hash(hasher, seed);
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
        return "function0: murmur3_128(" + seed + ")";
    }
}
