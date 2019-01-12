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
package io.prestosql.rcfile;

import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public interface RcFileCompressor
{
    CompressedSliceOutput createCompressedSliceOutput(int minChunkSize, int maxChunkSize);

    // This specialized SliceOutput has direct access buffered output slices to
    // report buffer sizes and to get he final output.  Additionally, a new
    // CompressedSliceOutput can be created that reuses the underlying output
    // buffer
    final class CompressedSliceOutput
            extends BufferedOutputStreamSliceOutput
    {
        private final ChunkedSliceOutput bufferedOutput;
        private final Supplier<CompressedSliceOutput> resetFactory;
        private final Runnable onDestroy;
        private boolean closed;
        private boolean destroyed;

        /**
         * @param compressionStream the compressed output stream to delegate to
         * @param bufferedOutput the output for the compressionStream
         * @param resetFactory the function to create a new CompressedSliceOutput that reuses the bufferedOutput
         * @param onDestroy used to cleanup the compression when done
         */
        public CompressedSliceOutput(OutputStream compressionStream, ChunkedSliceOutput bufferedOutput, Supplier<CompressedSliceOutput> resetFactory, Runnable onDestroy)
        {
            super(compressionStream);
            this.bufferedOutput = requireNonNull(bufferedOutput, "bufferedOutput is null");
            this.resetFactory = requireNonNull(resetFactory, "resetFactory is null");
            this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
        }

        @Override
        public long getRetainedSize()
        {
            return super.getRetainedSize() + bufferedOutput.getRetainedSize();
        }

        public int getCompressedSize()
        {
            checkState(closed, "Stream has not been closed");
            checkState(!destroyed, "Stream has been destroyed");
            return bufferedOutput.size();
        }

        public List<Slice> getCompressedSlices()
        {
            checkState(closed, "Stream has not been closed");
            checkState(!destroyed, "Stream has been destroyed");
            return bufferedOutput.getSlices();
        }

        public CompressedSliceOutput createRecycledCompressedSliceOutput()
        {
            checkState(closed, "Stream has not been closed");
            checkState(!destroyed, "Stream has been destroyed");
            destroyed = true;
            return resetFactory.get();
        }

        @Override
        public void close()
                throws IOException
        {
            if (!closed) {
                closed = true;
                super.close();
            }
        }

        public void destroy()
                throws IOException
        {
            if (!destroyed) {
                destroyed = true;
                try {
                    close();
                }
                finally {
                    onDestroy.run();
                }
            }
        }
    }
}
