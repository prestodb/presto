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
package com.facebook.presto.iceberg.function.changelog;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorState;

import static java.util.Objects.requireNonNull;

public interface ApplyChangelogState
        extends AccumulatorState
{
    Type getType();

    ChangelogRecord get();

    void set(ChangelogRecord value);

    class Single
            implements ApplyChangelogState
    {
        private final Type innerType;
        ChangelogRecord record;

        public Single(Type innerType)
        {
            this.innerType = innerType;
        }

        @Override
        public Type getType()
        {
            return innerType;
        }

        @Override
        public ChangelogRecord get()
        {
            return record;
        }

        @Override
        public void set(ChangelogRecord value)
        {
            record = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (record == null) {
                return 0;
            }
            return record.getEstimatedSize();
        }
    }

    class Grouped
            extends AbstractGroupedAccumulatorState
            implements ApplyChangelogState
    {
        private final ObjectBigArray<ChangelogRecord> records = new ObjectBigArray<>();
        private long size;
        private final Type innerType;

        public Grouped(Type innerType)
        {
            this.innerType = innerType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            records.ensureCapacity(size);
        }

        @Override
        public Type getType()
        {
            return innerType;
        }

        @Override
        public ChangelogRecord get()
        {
            return records.get(getGroupId());
        }

        @Override
        public void set(ChangelogRecord value)
        {
            requireNonNull(value, "value is null");

            ChangelogRecord previous = get();
            if (previous != null) {
                size -= previous.getEstimatedSize();
            }

            records.set(getGroupId(), value);
            size += value.getEstimatedSize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size + records.sizeOf();
        }
    }
}
