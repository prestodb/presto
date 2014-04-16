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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

public class ConcatRecordSet
        implements RecordSet
{
    private final Iterable<RecordSet> recordSets;
    private final List<Type> types;

    public ConcatRecordSet(Iterable<RecordSet> recordSets, List<Type> types)
    {
        this.recordSets = checkNotNull(recordSets, "recordSets is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        Iterable<RecordCursor> recordCursors = transform(recordSets, new Function<RecordSet, RecordCursor>()
        {
            @Override
            public RecordCursor apply(RecordSet recordSet)
            {
                checkState(recordSet.getColumnTypes().equals(types), "RecordSet types do not match declared types");
                return recordSet.cursor();
            }
        });
        return new ConcatRecordCursor(recordCursors.iterator(), types);
    }

    private static class ConcatRecordCursor
            implements RecordCursor
    {
        private final Iterator<RecordCursor> iterator;
        private final List<Type> types;

        private RecordCursor currentCursor;

        private ConcatRecordCursor(Iterator<RecordCursor> iterator, List<Type> types)
        {
            this.iterator = checkNotNull(iterator, "iterator is null");
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkPositionIndex(field, types.size());
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (currentCursor == null || !currentCursor.advanceNextPosition()) {
                if (!iterator.hasNext()) {
                    return false;
                }
                if (currentCursor != null) {
                    currentCursor.close();
                }
                currentCursor = iterator.next();
            }
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkPositionIndex(field, types.size());
            return currentCursor.getBoolean(field);
        }

        @Override
        public long getLong(int field)
        {
            checkPositionIndex(field, types.size());
            return currentCursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            checkPositionIndex(field, types.size());
            return currentCursor.getDouble(field);
        }

        @Override
        public byte[] getString(int field)
        {
            checkPositionIndex(field, types.size());
            return currentCursor.getString(field);
        }

        @Override
        public boolean isNull(int field)
        {
            checkPositionIndex(field, types.size());
            return currentCursor.isNull(field);
        }

        @Override
        public void close()
        {
            if (currentCursor != null) {
                currentCursor.close();
            }
            // Remaining iterator are lazily generated
        }
    }
}
