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
package com.facebook.presto.hive.pagefile;

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static java.util.Objects.requireNonNull;

public class PageFilePageReader
        implements Iterator<Page>
{
    private final PagesSerde pagesSerde;
    private final SliceInput input;
    private final long readLength;

    public PageFilePageReader(
            long readStart,
            long readLength,
            FSDataInputStream inputStream,
            PagesSerde pagesSerde)
            throws IOException
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.readLength = readLength;
        requireNonNull(inputStream, "inputStream is null");
        inputStream.seek(readStart);
        this.input = new InputStreamSliceInput(inputStream);
    }

    @Override
    public boolean hasNext()
    {
        return input.position() < readLength && input.isReadable();
    }

    @Override
    public Page next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        SerializedPage serializedPage = readSerializedPage(input);
        return pagesSerde.deserialize(serializedPage);
    }
}
