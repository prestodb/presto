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
package com.facebook.presto.orc.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnEncoding
{
    public enum ColumnEncodingKind
    {
        DIRECT,
        DICTIONARY,
        DIRECT_V2,
        DICTIONARY_V2,
        DWRF_DIRECT,
    }

    private final ColumnEncodingKind columnEncodingKind;
    private final int dictionarySize;

    public ColumnEncoding(ColumnEncodingKind columnEncodingKind, int dictionarySize)
    {
        this.columnEncodingKind = requireNonNull(columnEncodingKind, "columnEncodingKind is null");
        this.dictionarySize = dictionarySize;
    }

    public ColumnEncodingKind getColumnEncodingKind()
    {
        return columnEncodingKind;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnEncodingKind", columnEncodingKind)
                .add("dictionarySize", dictionarySize)
                .toString();
    }
}
