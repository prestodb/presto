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
package com.facebook.presto.cache.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;
import com.facebook.presto.hive.HiveFileContext;
import org.apache.hadoop.fs.FileStatus;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class AlluxioURIStatus
        extends URIStatus
{
    private final HiveFileContext hiveFileContext;

    private static FileInfo toAlluxioFileInfo(FileStatus status)
    {
        // FilePath is a unique identifier for a file, however it can be a long string
        // hence using md5 hash of the file path as the identifier in the cache.
        // We don't set fileId because fileId is Alluxio specific
        FileInfo info = new FileInfo();
        info.setFileIdentifier(md5().hashString(status.getPath().toString(), UTF_8).toString())
                .setLength(status.getLen())
                .setPath(status.getPath().toString())
                .setFolder(status.isDirectory())
                .setBlockSizeBytes(status.getBlockSize())
                .setLastModificationTimeMs(status.getModificationTime())
                .setLastAccessTimeMs(status.getAccessTime())
                .setOwner(status.getOwner())
                .setGroup(status.getGroup());
        return info;
    }

    public AlluxioURIStatus(FileStatus fileStatus, HiveFileContext hiveFileContext)
    {
        super(toAlluxioFileInfo(fileStatus));
        this.hiveFileContext = requireNonNull(hiveFileContext, "hiveFileContext is null");
    }

    public HiveFileContext getHiveFileContext()
    {
        return hiveFileContext;
    }
}
