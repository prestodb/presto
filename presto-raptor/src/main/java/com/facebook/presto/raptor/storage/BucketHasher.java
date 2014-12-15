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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.type.Type;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

public class BucketHasher
{
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

    private final int numShards;
    private final Hasher hasher = HASH_FUNCTION.newHasher();

    public BucketHasher(int numShards)
    {
        checkArgument(numShards > 0, "must have at least one shard");
        this.numShards = numShards;
    }

    public BucketHasher addLong(long value)
    {
        hasher.putLong(value);
        return this;
    }

    public BucketHasher addDouble(double value)
    {
        hasher.putDouble(value);
        return this;
    }

    public BucketHasher addBoolean(boolean value)
    {
        hasher.putBoolean(value);
        return this;
    }

    public BucketHasher addBytes(byte[] value)
    {
        hasher.putBytes(value);
        return this;
    }

    public int computeBucketId()
    {
        return positiveMod(hasher.hash().asInt(), numShards);
    }

    public BucketHasher addValue(TupleBuffer tuple, int field)
    {
        checkArgument(!tuple.isNull(field), "Cannot hash NULL values in field %s", field);
        Type type = tuple.getType(field);

        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) {
            return addLong(tuple.getLong(field));
        }
        if (javaType == double.class) {
            return addDouble(tuple.getDouble(field));
        }
        if (javaType == boolean.class) {
            return addBoolean(tuple.getBoolean(field));
        }
        if (javaType == Slice.class) {
            return addBytes(tuple.getSlice(field).getBytes());
        }
        throw new IllegalArgumentException("Unknown java type: " + javaType);
    }

    private static int positiveMod(int value, int mod)
    {
        return ((value % mod) + mod) % mod;
    }
}
