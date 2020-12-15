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
package com.facebook.presto.server;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.page.SerializedPage;
import io.airlift.slice.Slice;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.Slices.allocate;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class SerializedPageWriteListener
        implements WriteListener
{
    private static final Logger log = Logger.get(SerializedPageWriteListener.class);

    public static final int PAGE_METADATA_SIZE = SIZE_OF_INT * 3 + SIZE_OF_BYTE;
    private final ArrayDeque<SerializedPage> serializedPages;
    private final AsyncContext asyncContext;
    private final ServletOutputStream output;
    private final Slice slice;
    private SerializedPage page;

    public SerializedPageWriteListener(
            List<SerializedPage> serializedPages,
            AsyncContext asyncContext,
            ServletOutputStream output)
    {
        this.serializedPages = new ArrayDeque<>(requireNonNull(serializedPages, "serializedPages is null"));
        this.asyncContext = requireNonNull(asyncContext, "asyncContext is null");
        this.output = requireNonNull(output, "output is null");
        this.slice = allocate(PAGE_METADATA_SIZE);
    }

    @Override
    public void onWritePossible()
            throws IOException
    {
        while (output.isReady()) {
            if (writeComplete()) {
                asyncContext.complete();
                return;
            }

            if (page == null) {
                page = serializedPages.poll();

                int bufferPosition = 0;

                slice.setInt(bufferPosition, page.getPositionCount());
                bufferPosition += SIZE_OF_INT;
                slice.setByte(bufferPosition, page.getPageCodecMarkers());
                bufferPosition += SIZE_OF_BYTE;
                slice.setInt(bufferPosition, page.getUncompressedSizeInBytes());
                bufferPosition += SIZE_OF_INT;
                slice.setInt(bufferPosition, page.getSizeInBytes());
                bufferPosition += SIZE_OF_INT;

                output.write(slice.byteArray(), 0, bufferPosition);
            }
            else {
                Object base = page.getSlice().getBase();
                checkArgument(base instanceof byte[], "serialization type only supports byte[]");
                output.write((byte[]) base, (int) ((page.getSlice().getAddress() - ARRAY_BYTE_BASE_OFFSET)), page.getSizeInBytes());
                page = null;
            }
        }
    }

    @Override
    public void onError(Throwable t)
    {
        log.error(t);
        asyncContext.complete();
    }

    private boolean writeComplete()
    {
        return serializedPages.isEmpty() && page == null;
    }
}
