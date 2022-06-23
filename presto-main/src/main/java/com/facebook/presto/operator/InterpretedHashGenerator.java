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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.type.TypeUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InterpretedHashGenerator
        implements HashGenerator
{
    private final Type[] hashChannelTypes;
    @Nullable
    private final int[] hashChannels; // null value indicates that the identity channel mapping is used

    public static InterpretedHashGenerator createPositionalWithTypes(List<Type> hashChannelTypes)
    {
        return new InterpretedHashGenerator(hashChannelTypes, null, true);
    }

    public InterpretedHashGenerator(List<Type> hashChannelTypes, List<Integer> hashChannels)
    {
        this(hashChannelTypes, requireNonNull(hashChannels).stream().mapToInt(i -> i).toArray());
    }

    public InterpretedHashGenerator(List<Type> hashChannelTypes, int[] hashChannels)
    {
        this(hashChannelTypes, requireNonNull(hashChannels, "hashChannels is null"), false);
    }

    private InterpretedHashGenerator(List<Type> hashChannelTypes, @Nullable int[] hashChannels, boolean positional)
    {
        this.hashChannelTypes = requireNonNull(hashChannelTypes, "hashChannelTypes is null").toArray(new Type[0]);
        if (positional) {
            checkArgument(hashChannels == null, "hashChannels must be null");
            this.hashChannels = null;
        }
        else {
            requireNonNull(hashChannels, "hashChannels is null");
            checkArgument(hashChannels.length == this.hashChannelTypes.length);
            // simple positional indices are converted to null
            this.hashChannels = isPositionalChannels(hashChannels) ? null : hashChannels;
        }
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, IntFunction<Block> blockProvider)
        long result = INITIAL_HASH_VALUE;
        for (int i = 0; i < hashChannelTypes.length; i++) {
            Block block = page.getBlock(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, TypeUtils.hashPosition(hashChannelTypes[i], block, position));
        }
        return result;
    }

    public long hashPosition(int position, IntFunction<Block> blockProvider)
    {
        // Note: this code is duplicated for performance but must logically match hashPosition(position, Page page)
        long result = INITIAL_HASH_VALUE;
        for (int i = 0; i < hashChannelTypes.length; i++) {
            Block block = blockProvider.apply(hashChannels == null ? i : hashChannels[i]);
            result = CombineHashFunction.getHash(result, TypeUtils.hashPosition(hashChannelTypes[i], block, position));
        }
        return result;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hashChannelTypes", hashChannelTypes)
                .add("hashChannels", hashChannels == null ? "<identity>" : Arrays.toString(hashChannels))
                .toString();
    }

    private static boolean isPositionalChannels(int[] hashChannels)
    {
        for (int i = 0; i < hashChannels.length; i++) {
            if (hashChannels[i] != i) {
                return false;
            }
        }
        return true;
    }
}
