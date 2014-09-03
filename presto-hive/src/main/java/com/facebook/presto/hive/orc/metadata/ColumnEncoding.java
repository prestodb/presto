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

public class ColumnEncoding
{
    public enum Kind
    {
        DIRECT,
        DICTIONARY,
        DIRECT_V2,
        DICTIONARY_V2,
    }

    private final Kind kind;
    private final int dictionarySize;

    public ColumnEncoding(Kind kind, int dictionarySize)
    {
        this.kind = kind;
        this.dictionarySize = dictionarySize;
    }

    public Kind getKind()
    {
        return kind;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("kind", kind)
                .add("dictionarySize", dictionarySize)
                .toString();
    }
}
