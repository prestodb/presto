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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PageSourceRecordSet
        implements RecordSet
{
    final List<Type> types;
    final ConnectorPageSource pageSource;

    public PageSourceRecordSet(IndexPageSource pageSource)
    {
        this(pageSource.getColumnTypes(), pageSource);
    }

    public PageSourceRecordSet(List<Type> types, ConnectorPageSource pageSource)
    {
        this.types = requireNonNull(types, "types is null");
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new PageSourceRecordCursor(types, pageSource);
    }

    public static class PageSourceRecordCursor
            implements RecordCursor
    {
        final List<Type> types;
        final ConnectorPageSource pageSource;
        private Page page;
        private int position = -1;

        private PageSourceRecordCursor(List<Type> types, ConnectorPageSource pageSource)
        {
            this.types = requireNonNull(types, "types is null");
            this.pageSource = requireNonNull(pageSource, "pageSource is null");
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
            if (page == null || position >= page.getPositionCount()) {
                position = 0;
                return fetchNextPage();
            }
            return true;
        }

        private boolean fetchNextPage()
        {
            page = pageSource.getNextPage();
            return page != null;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return types.get(field).getBoolean(page.getBlock(field), position);
        }

        @Override
        public long getLong(int field)
        {
            return types.get(field).getLong(page.getBlock(field), position);
        }

        @Override
        public double getDouble(int field)
        {
            return types.get(field).getDouble(page.getBlock(field), position);
        }

        @Override
        public Slice getSlice(int field)
        {
            return types.get(field).getSlice(page.getBlock(field), position);
        }

        @Override
        public Object getObject(int field)
        {
            return types.get(field).getObject(page.getBlock(field), position);
        }

        @Override
        public boolean isNull(int field)
        {
            requireNonNull(page);
            return page.getBlock(field).isNull(position);
        }

        @Override
        public void close()
        {
            try {
                pageSource.close();
            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
    }
}
