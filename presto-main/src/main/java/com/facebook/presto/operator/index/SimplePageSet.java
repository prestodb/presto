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
package com.facebook.presto.operator.index;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SimplePageSet
        implements PageSet
{
    private final List<Page> pages;
    private final List<Type> types;

    public SimplePageSet(List<Type> types, List<Page> pages)
    {
        this.types = requireNonNull(types, "types is null");
        this.pages = requireNonNull(pages, "types is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public List<Page> getPages()
    {
        return pages;
    }

    public static SimplePageSet fromRecordSet(RecordSet recordSet)
    {
        PageBuilder pageBuilder = new PageBuilder(recordSet.getColumnTypes());
        ImmutableList.Builder<Page> pageListBuilder = ImmutableList.builder();
        RecordCursor recordCursor = recordSet.cursor();
        List<Type> types = recordSet.getColumnTypes();
        while (recordCursor.advanceNextPosition()) {
            if (pageBuilder.isFull()) {
                pageListBuilder.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageBuilder.declarePosition();

            for (int column = 0; column < types.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                if (recordCursor.isNull(column)) {
                    output.appendNull();
                }
                else {
                    Type type = types.get(column);
                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        type.writeBoolean(output, recordCursor.getBoolean(column));
                    }
                    else if (javaType == long.class) {
                        type.writeLong(output, recordCursor.getLong(column));
                    }
                    else if (javaType == double.class) {
                        type.writeDouble(output, recordCursor.getDouble(column));
                    }
                    else if (javaType == Slice.class) {
                        Slice slice = recordCursor.getSlice(column);
                        type.writeSlice(output, slice, 0, slice.length());
                    }
                    else {
                        type.writeObject(output, recordCursor.getObject(column));
                    }
                }
            }
        }
        if (!pageBuilder.isEmpty()) {
            pageListBuilder.add(pageBuilder.build());
        }
        return new SimplePageSet(types, pageListBuilder.build());
    }
}
