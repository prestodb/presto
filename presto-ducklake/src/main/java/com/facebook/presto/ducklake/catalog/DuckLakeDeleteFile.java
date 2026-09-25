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
package com.facebook.presto.ducklake.catalog;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * A visible-at-snapshot row of {@code ducklake_delete_file}, attached to the {@code
 * ducklake_data_file} row it deletes rows from via {@code data_file_id}. {@code path} has already
 * been resolved by the JDBC layer to an absolute location.
 */
public class DuckLakeDeleteFile
{
    private final long deleteFileId;
    private final String path;
    private final String format;
    private final long deleteCount;
    private final long fileSizeBytes;
    private final OptionalLong footerSize;
    private final Optional<String> encryptionKey;
    private final OptionalLong partialMax;

    public DuckLakeDeleteFile(
            long deleteFileId,
            String path,
            String format,
            long deleteCount,
            long fileSizeBytes,
            OptionalLong footerSize,
            Optional<String> encryptionKey,
            OptionalLong partialMax)
    {
        this.deleteFileId = deleteFileId;
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
        this.deleteCount = deleteCount;
        this.fileSizeBytes = fileSizeBytes;
        this.footerSize = requireNonNull(footerSize, "footerSize is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        this.partialMax = requireNonNull(partialMax, "partialMax is null");
    }

    public long getDeleteFileId()
    {
        return deleteFileId;
    }

    public String getPath()
    {
        return path;
    }

    public String getFormat()
    {
        return format;
    }

    public long getDeleteCount()
    {
        return deleteCount;
    }

    public long getFileSizeBytes()
    {
        return fileSizeBytes;
    }

    public OptionalLong getFooterSize()
    {
        return footerSize;
    }

    public Optional<String> getEncryptionKey()
    {
        return encryptionKey;
    }

    public OptionalLong getPartialMax()
    {
        return partialMax;
    }
}
