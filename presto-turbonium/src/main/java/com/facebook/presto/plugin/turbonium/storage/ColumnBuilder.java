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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;

public interface ColumnBuilder
{
    int getChannel();
    Type getType();
    void appendPage(Page page);
    Column build();

    static ColumnBuilder create(int channel, Type type, boolean disableEncoding)
    {
        if (type instanceof FixedWidthType) {
            int size = ((FixedWidthType) type).getFixedSize();
            switch (size) {
                case 1:
                    if (type.getJavaType() == boolean.class) {
                        return new BooleanColumnBuilder(channel, type, disableEncoding);
                    }
                    else {
                        return new ByteColumnBuilder(channel, type, disableEncoding);
                    }
                case 2:
                    return new ShortColumnBuilder(channel, type, disableEncoding);
                case 4:
                    return new IntColumnBuilder(channel, type, disableEncoding);
                case 8:
                    if (type.getJavaType() == double.class) {
                        return new DoubleColumnBuilder(channel, type, disableEncoding);
                    }
                    else {
                        return new LongColumnBuilder(channel, type, disableEncoding);
                    }
                case 16:
                    return new SliceColumnBuilder(channel, type, disableEncoding);
                default:
                    throw new IllegalArgumentException(String.format("Unsupported segmentCount: %s", size));
            }
        }
        else {
            return new SliceColumnBuilder(channel, type, disableEncoding);
        }
    }
}
