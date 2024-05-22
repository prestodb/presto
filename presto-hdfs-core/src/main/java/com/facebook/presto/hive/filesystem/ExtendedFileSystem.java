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
package com.facebook.presto.hive.filesystem;

import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public abstract class ExtendedFileSystem
        extends FileSystem
{
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        return open(path);
    }

    public RemoteIterator<HiveFileInfo> listFiles(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public RemoteIterator<LocatedFileStatus> listDirectory(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public ListenableFuture<Void> renameFileAsync(Path source, Path destination)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
