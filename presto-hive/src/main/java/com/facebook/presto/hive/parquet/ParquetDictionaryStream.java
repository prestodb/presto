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
package com.facebook.presto.hive.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.format.DictionaryPageHeader;
import parquet.format.PageHeader;
import parquet.format.PageType;
import parquet.format.Util;

public class ParquetDictionaryStream
    extends ByteArrayInputStream
{
    public ParquetDictionaryStream(byte[] data, int offset)
    {
        super(data);
        this.pos = offset;
    }

    protected PageHeader readPageHeader()
        throws IOException
    {
        return Util.readPageHeader(this);
    }

    public DictionaryPage readDictionaryPage()
        throws IOException
    {
        PageHeader pageHeader = readPageHeader();
        int uncompressedPageSize = pageHeader.getUncompressed_page_size();
        int compressedPageSize = pageHeader.getCompressed_page_size();
        if (pageHeader.type == PageType.DICTIONARY_PAGE) {
            DictionaryPageHeader dicHeader = pageHeader.getDictionary_page_header();
            return new DictionaryPage(getBytesInput(compressedPageSize),
                                        uncompressedPageSize,
                                        dicHeader.getNum_values(),
                                        Encoding.valueOf(dicHeader.getEncoding().name()));
        }
        return null;
    }

    public int getPosition()
    {
        return this.pos;
    }

    public BytesInput getBytesInput(int size)
        throws IOException
    {
        final BytesInput bytesInput = BytesInput.from(this.buf, this.pos, size);
        this.pos += size;
        return bytesInput;
    }
}
