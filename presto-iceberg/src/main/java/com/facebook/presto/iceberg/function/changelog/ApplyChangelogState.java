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
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import com.facebook.presto.spi.function.GroupedAccumulatorState;

import javax.annotation.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@AccumulatorStateMetadata(stateSerializerClass = ApplyChangelogStateSerializer.class, stateFactoryClass = ApplyChangelogStateFactory.class)
public interface ApplyChangelogState
        extends AccumulatorState
{
    public Type getType();

    Optional<ChangelogRecord> getRecord();

    void setRecord(@Nullable ChangelogRecord value);

    class Single
            implements ApplyChangelogState
    {
        private final Type innerType;
        private ChangelogRecord record;

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
        public Optional<ChangelogRecord> getRecord()
        {
            return Optional.ofNullable(record);
        }

        @Override
        public void setRecord(ChangelogRecord value)
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
            implements ApplyChangelogState, GroupedAccumulatorState
    {
        private final ObjectBigArray<ChangelogRecord> records = new ObjectBigArray<>();
        private long size;
        private final Type innerType;
        private long groupId;

        public Grouped(Type innerType)
        {
            this.innerType = innerType;
        }

        protected final long getGroupId()
        {
            return groupId;
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
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
        public Optional<ChangelogRecord> getRecord()
        {
            return Optional.ofNullable(records.get(getGroupId()));
        }

        @Override
        public void setRecord(ChangelogRecord value)
        {
            requireNonNull(value, "value is null");

            size -= getRecord().map(ChangelogRecord::getEstimatedSize).orElse(0L);
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
