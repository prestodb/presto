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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import io.airlift.units.DataSize;

import java.util.Optional;

import static com.facebook.presto.orc.FlushReason.DICTIONARY_FULL;
import static com.facebook.presto.orc.FlushReason.MAX_BYTES;
import static com.facebook.presto.orc.FlushReason.MAX_ROWS;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DefaultOrcWriterFlushPolicy
        implements OrcWriterFlushPolicy
{
    public static final DataSize DEFAULT_STRIPE_MIN_SIZE = new DataSize(32, MEGABYTE);
    public static final DataSize DEFAULT_STRIPE_MAX_SIZE = new DataSize(64, MEGABYTE);
    public static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;

    private final int stripeMaxRowCount;
    private final int stripeMinBytes;
    private final int stripeMaxBytes;

    private DefaultOrcWriterFlushPolicy(int stripeMaxRowCount, int stripeMinBytes, int stripeMaxBytes)
    {
        this.stripeMaxRowCount = stripeMaxRowCount;
        this.stripeMinBytes = stripeMinBytes;
        this.stripeMaxBytes = stripeMaxBytes;
    }

    @Override
    public Optional<FlushReason> shouldFlushStripe(int stripeRowCount, int bufferedBytes, boolean dictionaryIsFull)
    {
        if (stripeRowCount == stripeMaxRowCount) {
            return Optional.of(MAX_ROWS);
        }
        else if (bufferedBytes > stripeMaxBytes) {
            return Optional.of(MAX_BYTES);
        }
        else if (dictionaryIsFull) {
            return Optional.of(DICTIONARY_FULL);
        }
        return Optional.empty();
    }

    @Override
    public int getMaxChunkRowCount(Page page)
    {
        // avoid chunks with huge logical size
        int chunkMaxLogicalBytes = max(1, stripeMaxBytes / 2);
        double averageLogicalSizePerRow = (double) page.getApproximateLogicalSizeInBytes() / page.getPositionCount();
        return max(1, (int) (chunkMaxLogicalBytes / max(1, averageLogicalSizePerRow)));
    }

    @Override
    public int getStripeMinBytes()
    {
        return stripeMinBytes;
    }

    @Override
    public int getStripeMaxBytes()
    {
        return stripeMaxBytes;
    }

    @Override
    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stripeMaxRowCount", stripeMaxRowCount)
                .add("stripeMinBytes", stripeMinBytes)
                .add("stripeMaxBytes", stripeMaxBytes)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int stripeMaxRowCount = DEFAULT_STRIPE_MAX_ROW_COUNT;
        private DataSize stripeMinSize = DEFAULT_STRIPE_MIN_SIZE;
        private DataSize stripeMaxSize = DEFAULT_STRIPE_MAX_SIZE;

        private Builder() {}

        public Builder withStripeMaxRowCount(int stripeMaxRowCount)
        {
            checkArgument(stripeMaxRowCount >= 1, "stripeMaxRowCount must be at least 1");
            this.stripeMaxRowCount = stripeMaxRowCount;
            return this;
        }

        public Builder withStripeMinSize(DataSize stripeMinSize)
        {
            this.stripeMinSize = requireNonNull(stripeMinSize, "stripeMinSize is null");
            return this;
        }

        public Builder withStripeMaxSize(DataSize stripeMaxSize)
        {
            this.stripeMaxSize = requireNonNull(stripeMaxSize, "stripeMaxSize is null");
            return this;
        }

        public DefaultOrcWriterFlushPolicy build()
        {
            checkArgument(stripeMaxSize.compareTo(stripeMinSize) >= 0, "stripeMaxSize must be greater than stripeMinSize");
            return new DefaultOrcWriterFlushPolicy(
                    stripeMaxRowCount,
                    toIntExact(stripeMinSize.toBytes()),
                    toIntExact(stripeMaxSize.toBytes()));
        }
    }
}
