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
package com.facebook.presto.spi.spiller;

import com.facebook.presto.common.io.DataSink;
import io.airlift.slice.SliceInput;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * An interface for temporary file storage. A temporary file store can be used for the following purpose:
 * - Spilling
 * - In Presto-on-Spark, broadcast join can be implemented via writing the build table into a temporary table
 *   and read back on all executors.
 *
 * Note:
 * - For the spilling use case, the TempFileStore can be either a local filesystem or a disaggregated file system (e.g. S3).
 * - For the broadcast join use case, the TempFileStore has to be a disaggregated file system; and the temporary file can be read
 *      from different workers.
 */
public interface TempFileStore
{
    void initialize();

    TempFile createTemporaryFile()
            throws IOException;

    SliceInput getSliceInput(String handle)
            throws IOException;

    void removeTemporaryFile(String handle);

    // Hack!
    int getFileBufferSize();

    class TempFile
    {
        private final String handle;
        private final DataSink dataSink;

        public TempFile(String handle, DataSink dataSink)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.dataSink = requireNonNull(dataSink, "dataSink is null");
        }

        public String getHandle()
        {
            return handle;
        }

        public DataSink getDataSink()
        {
            return dataSink;
        }
    }
}
