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
package com.facebook.presto.parquet.batchreader.decoders.delta;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import org.apache.parquet.column.values.ValuesReader;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractInt64AndInt32ShortDecimalDeltaValuesDecoder
        implements ShortDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractInt64AndInt32ShortDecimalDeltaValuesDecoder.class).instanceSize();

    protected final ValuesReader delegate;

    public AbstractInt64AndInt32ShortDecimalDeltaValuesDecoder(ValuesReader delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++) {
            values[i] = readData();
        }
    }

    protected abstract long readData();

    @Override
    public void skip(int length)
    {
        checkArgument(length >= 0, "invalid length %s", length);
        delegate.skip(length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
