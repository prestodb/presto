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
package com.facebook.presto.spi.page;

import com.facebook.presto.common.io.DataOutput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.page.PagesSerdeUtil.PAGE_METADATA_SIZE;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static java.util.Objects.requireNonNull;

public class PageDataOutput
        implements DataOutput
{
    private final SerializedPage serializedPage;

    public PageDataOutput(SerializedPage serializedPage)
    {
        this.serializedPage = requireNonNull(serializedPage, "serializedPage is null");
    }

    @Override
    public long size()
    {
        return PAGE_METADATA_SIZE + serializedPage.getSizeInBytes();
    }

    @Override
    public void writeData(SliceOutput sliceOutput)
    {
        writeSerializedPage(sliceOutput, serializedPage);
    }
}
