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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;

public class Stream
{
    public enum Kind
    {
        PRESENT,
        DATA,
        LENGTH,
        DICTIONARY_DATA,
        DICTIONARY_COUNT,
        SECONDARY,
        ROW_INDEX,
//        NANO_DATA,
        IN_DICTIONARY,
        STRIDE_DICTIONARY,
        STRIDE_DICTIONARY_LENGTH,
    }

    private final int column;
    private final Kind kind;
    private final int length;
    private final boolean useVInts;

    public Stream(int column, Kind kind, int length, boolean useVInts)
    {
        this.column = column;
        this.kind = kind;
        this.length = length;
        this.useVInts = useVInts;
    }

    public int getColumn()
    {
        return column;
    }

    public Kind getKind()
    {
        return kind;
    }

    public int getLength()
    {
        return length;
    }

    public boolean isUseVInts()
    {
        return useVInts;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("column", column)
                .add("kind", kind)
                .add("length", length)
                .add("useVInts", useVInts)
                .toString();
    }
}
