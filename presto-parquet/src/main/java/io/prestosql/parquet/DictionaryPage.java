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
package io.prestosql.parquet;

import io.airlift.slice.Slice;

import java.util.Arrays;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class DictionaryPage
        extends Page
{
    private final Slice slice;
    private final int dictionarySize;
    private final ParquetEncoding encoding;

    public DictionaryPage(Slice slice, int dictionarySize, ParquetEncoding encoding)
    {
        this(requireNonNull(slice, "slice is null"),
                slice.length(),
                dictionarySize,
                requireNonNull(encoding, "encoding is null"));
    }

    public DictionaryPage(Slice slice, int uncompressedSize, int dictionarySize, ParquetEncoding encoding)
    {
        super(requireNonNull(slice, "slice is null").length(), uncompressedSize);
        this.slice = slice;
        this.dictionarySize = dictionarySize;
        this.encoding = requireNonNull(encoding, "encoding is null");
    }

    public Slice getSlice()
    {
        return slice;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    public ParquetEncoding getEncoding()
    {
        return encoding;
    }

    public DictionaryPage copy()
    {
        return new DictionaryPage(wrappedBuffer(Arrays.copyOf(slice.getBytes(), slice.length())), getUncompressedSize(), dictionarySize, encoding);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("slice", slice)
                .add("dictionarySize", dictionarySize)
                .add("encoding", encoding)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }
}
