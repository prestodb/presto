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
package com.facebook.presto.cache.filemerge;

import com.facebook.presto.cache.CacheManager;
import com.facebook.presto.cache.FileReadRequest;
import com.facebook.presto.hive.CacheQuota;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public final class FileMergeCachingInputStream
        extends FSDataInputStream
{
    private final FSDataInputStream inputStream;
    private final CacheManager cacheManager;
    private final Path path;
    private final CacheQuota cacheQuota;
    private final boolean cacheValidationEnabled;

    public FileMergeCachingInputStream(
            FSDataInputStream inputStream,
            CacheManager cacheManager,
            Path path,
            CacheQuota cacheQuota,
            boolean cacheValidationEnabled)
    {
        super(inputStream);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.path = requireNonNull(path, "path is null");
        this.cacheQuota = requireNonNull(cacheQuota, "cacheQuota is null");
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        FileReadRequest key = new FileReadRequest(path, position, length);
        switch (cacheManager.get(key, buffer, offset, cacheQuota)) {
            case HIT:
                break;
            case MISS:
                inputStream.readFully(position, buffer, offset, length);
                cacheManager.put(key, wrappedBuffer(buffer, offset, length), cacheQuota);
                return;
            case CACHE_QUOTA_EXCEED:
                inputStream.readFully(position, buffer, offset, length);
                return;
        }

        if (cacheValidationEnabled) {
            byte[] validationBuffer = new byte[length];
            inputStream.readFully(position, validationBuffer, 0, length);
            for (int i = 0; i < length; i++) {
                verify(buffer[offset + i] == validationBuffer[i], "corrupted buffer at position " + i);
            }
        }
    }
}
