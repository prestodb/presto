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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.orc.OrcDataSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public interface OrcDataEnvironment
{
    FileSystem getFileSystem(HdfsContext context);

    OrcDataSource createOrcDataSource(FileSystem fileSystem, Path path, ReaderAttributes readerAttributes)
            throws IOException;

    DataSink createOrcDataSink(FileSystem fileSystem, Path path)
            throws IOException;
}
