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
package io.prestosql.tests.tpch;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

class ConcatRecordSet
        implements RecordSet
{
    private final Iterable<RecordSet> recordSets;
    private final List<Type> types;

    public ConcatRecordSet(Iterable<RecordSet> recordSets, List<Type> types)
    {
        this.recordSets = requireNonNull(recordSets, "recordSets is null");
        for (RecordSet recordSet : recordSets) {
            checkState(recordSet.getColumnTypes().equals(types), "RecordSet types do not match declared types");
        }
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        // NOTE: the ConcatRecordCursor implementation relies on the fact that the
        // cursor creation in the Iterable is lazy so DO NOT materialize this into
        // an ImmutableList
        Iterable<RecordCursor> recordCursors = transform(recordSets, RecordSet::cursor);
        return new ConcatRecordCursor(recordCursors.iterator(), types);
    }

    private static class ConcatRecordCursor
            implements RecordCursor
    {
        private final Iterator<RecordCursor> iterator;
        private final List<Type> types;

        private RecordCursor currentCursor;
        private boolean closed;

        private ConcatRecordCursor(Iterator<RecordCursor> iterator, List<Type> types)
        {
            // NOTE: this cursor implementation relies on the fact that the
            // cursor creation in the Iterable is lazy so DO NOT materialize this into
            // an ImmutableList
            this.iterator = requireNonNull(iterator, "iterator is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
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
            checkState(!closed);
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
            checkState(!closed);
            checkPositionIndex(field, types.size());
            return currentCursor.getBoolean(field);
        }

        @Override
        public long getLong(int field)
        {
            checkState(!closed);
            checkPositionIndex(field, types.size());
            return currentCursor.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(!closed);
            checkPositionIndex(field, types.size());
            return currentCursor.getDouble(field);
        }

        @Override
        public io.airlift.slice.Slice getSlice(int field)
        {
            checkState(!closed);
            checkPositionIndex(field, types.size());
            return currentCursor.getSlice(field);
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(!closed);
            checkPositionIndex(field, types.size());
            return currentCursor.isNull(field);
        }

        @Override
        public void close()
        {
            if (currentCursor != null) {
                currentCursor.close();
                currentCursor = null;
            }
            closed = true;
            // Remaining iterator are lazily generated
        }
    }
}
