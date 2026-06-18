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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import org.apache.iceberg.io.ByteBufferInputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.io.IOUtil.readRemaining;

public class HdfsCachedInputFile
        implements InputFile
{
    private static final Logger LOG = Logger.get(HdfsCachedInputFile.class);

    private final InputFile delegate;
    private final ManifestFileCacheKey cacheKey;
    private final ManifestFileCache cache;

    public HdfsCachedInputFile(InputFile delegate, ManifestFileCacheKey cacheKey, ManifestFileCache cache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cacheKey = requireNonNull(cacheKey, "cacheKey is null");
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Override
    public long getLength()
    {
        ManifestFileCachedContent cachedContent = cache.getIfPresent(cacheKey);
        if (cachedContent != null) {
            return cachedContent.getLength();
        }
        return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream()
    {
        ManifestFileCachedContent cachedContent = cache.getIfPresent(cacheKey);
        if (cachedContent != null) {
            return ByteBufferInputStream.wrap(cachedContent.getData());
        }

        long fileLength = delegate.getLength();
        if (cache.isEnabled() && fileLength <= cache.getMaxFileLength()) {
            try {
                ManifestFileCachedContent content = readFully(delegate, fileLength, cache.getBufferChunkSize());
                cache.put(cacheKey, content);
                cache.recordFileSize(content.getLength());
                return ByteBufferInputStream.wrap(content.getData());
            }
            catch (IOException e) {
                LOG.warn("Failed to cache input file: {}. Falling back to direct read.", delegate.location(), e);
            }
        }

        return delegate.newStream();
    }

    @Override
    public String location()
    {
        return delegate.location();
    }

    @Override
    public boolean exists()
    {
        return cache.getIfPresent(cacheKey) != null || delegate.exists();
    }

    private static ManifestFileCachedContent readFully(InputFile input, long fileLength, long chunkSize)
            throws IOException
    {
        try (SeekableInputStream stream = input.newStream()) {
            long totalBytesToRead = fileLength;
            List<ByteBuffer> buffers = new ArrayList<>(
                    ((int) (fileLength / chunkSize)) +
                            (fileLength % chunkSize == 0 ? 0 : 1));

            while (totalBytesToRead > 0) {
                int bytesToRead = (int) Math.min(chunkSize, totalBytesToRead);
                byte[] buf = new byte[bytesToRead];
                int bytesRead = readRemaining(stream, buf, 0, bytesToRead);
                totalBytesToRead -= bytesRead;

                if (bytesRead < bytesToRead) {
                    throw new IOException(
                            format("Failed to read %d bytes from file %s : %d bytes read.",
                                    fileLength, input.location(), fileLength - totalBytesToRead));
                }
                else {
                    buffers.add(ByteBuffer.wrap(buf));
                }
            }
            return new ManifestFileCachedContent(buffers, fileLength);
        }
    }
}
