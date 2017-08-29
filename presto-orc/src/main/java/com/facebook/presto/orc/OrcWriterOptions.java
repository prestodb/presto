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

import io.airlift.units.DataSize;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class OrcWriterOptions
{
    private static final DataSize DEFAULT_STRIPE_MAX_SIZE = new DataSize(64, MEGABYTE);
    private static final int DEFAULT_STRIPE_MIN_ROW_COUNT = 100_000;
    private static final int DEFAULT_STRIPE_MAX_ROW_COUNT = 10_000_000;
    private static final int DEFAULT_ROW_GROUP_MAX_ROW_COUNT = 10_000;
    private static final DataSize DEFAULT_DICTIONARY_MAX_MEMORY = new DataSize(16, MEGABYTE);

    private final DataSize stripeMaxSize;
    private final int stripeMinRowCount;
    private final int stripeMaxRowCount;
    private final int rowGroupMaxRowCount;
    private final DataSize dictionaryMaxMemory;

    public OrcWriterOptions()
    {
        this(
                DEFAULT_STRIPE_MAX_SIZE,
                DEFAULT_STRIPE_MIN_ROW_COUNT,
                DEFAULT_STRIPE_MAX_ROW_COUNT,
                DEFAULT_ROW_GROUP_MAX_ROW_COUNT,
                DEFAULT_DICTIONARY_MAX_MEMORY);
    }

    private OrcWriterOptions(DataSize stripeMaxSize, int stripeMinRowCount, int stripeMaxRowCount, int rowGroupMaxRowCount, DataSize dictionaryMaxMemory)
    {
        requireNonNull(stripeMaxSize, "stripeMaxSize is null");
        checkArgument(stripeMinRowCount >= 1, "stripeMinRowCount must be at least 1");
        checkArgument(stripeMaxRowCount >= 1, "stripeMaxRowCount must be at least 1");
        checkArgument(rowGroupMaxRowCount >= 1, "rowGroupMaxRowCount must be at least 1");
        requireNonNull(dictionaryMaxMemory, "dictionaryMaxMemory is null");

        this.stripeMaxSize = stripeMaxSize;
        this.stripeMinRowCount = stripeMinRowCount;
        this.stripeMaxRowCount = stripeMaxRowCount;
        this.rowGroupMaxRowCount = rowGroupMaxRowCount;
        this.dictionaryMaxMemory = dictionaryMaxMemory;
    }

    public DataSize getStripeMaxSize()
    {
        return stripeMaxSize;
    }

    public int getStripeMinRowCount()
    {
        return stripeMinRowCount;
    }

    public int getStripeMaxRowCount()
    {
        return stripeMaxRowCount;
    }

    public int getRowGroupMaxRowCount()
    {
        return rowGroupMaxRowCount;
    }

    public DataSize getDictionaryMaxMemory()
    {
        return dictionaryMaxMemory;
    }

    public OrcWriterOptions withStripeMaxSize(DataSize stripeMaxSize)
    {
        return new OrcWriterOptions(stripeMaxSize, stripeMinRowCount, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory);
    }

    public OrcWriterOptions withStripeMinRowCount(int stripeMinRowCount)
    {
        return new OrcWriterOptions(stripeMaxSize, stripeMinRowCount, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory);
    }

    public OrcWriterOptions withStripeMaxRowCount(int stripeMaxRowCount)
    {
        return new OrcWriterOptions(stripeMaxSize, stripeMinRowCount, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory);
    }

    public OrcWriterOptions withRowGroupMaxRowCount(int rowGroupMaxRowCount)
    {
        return new OrcWriterOptions(stripeMaxSize, stripeMinRowCount, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory);
    }

    public OrcWriterOptions withDictionaryMaxMemory(DataSize dictionaryMaxMemory)
    {
        return new OrcWriterOptions(stripeMaxSize, stripeMinRowCount, stripeMaxRowCount, rowGroupMaxRowCount, dictionaryMaxMemory);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stripeMaxSize", stripeMaxSize)
                .add("stripeMinRowCount", stripeMinRowCount)
                .add("stripeMaxRowCount", stripeMaxRowCount)
                .add("rowGroupMaxRowCount", rowGroupMaxRowCount)
                .add("dictionaryMaxMemory", dictionaryMaxMemory)
                .toString();
    }
}
