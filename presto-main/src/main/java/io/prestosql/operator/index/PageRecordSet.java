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
package io.prestosql.operator.index;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PageRecordSet
        implements RecordSet
{
    private final List<Type> types;
    private final Page page;

    public PageRecordSet(List<Type> types, Page page)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.page = requireNonNull(page, "page is null");
        checkArgument(types.size() == page.getChannelCount(), "Types do not match page channels");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new PageRecordCursor(types, page);
    }

    public static class PageRecordCursor
            implements RecordCursor
    {
        private final List<Type> types;
        private final Page page;
        private int position = -1;

        private PageRecordCursor(List<Type> types, Page page)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.page = requireNonNull(page, "page is null");
            checkArgument(types.size() == page.getChannelCount(), "Types do not match page channels");
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
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            position++;
            if (position >= page.getPositionCount()) {
                return false;
            }
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getBoolean(page.getBlock(field), position);
        }

        @Override
        public long getLong(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getLong(page.getBlock(field), position);
        }

        @Override
        public double getDouble(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getDouble(page.getBlock(field), position);
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getSlice(page.getBlock(field), position);
        }

        @Override
        public Object getObject(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            Type type = types.get(field);
            return type.getObject(page.getBlock(field), position);
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(position >= 0, "Not yet advanced");
            checkState(position < page.getPositionCount(), "Already finished");
            return page.getBlock(field).isNull(position);
        }

        @Override
        public void close()
        {
        }
    }
}
