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
package com.facebook.presto.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HdfsEnvironment
{
    private final HdfsConfiguration hdfsConfiguration;
    private final FileSystemWrapper fileSystemWrapper;

    @Inject
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, FileSystemWrapper fileSystemWrapper)
    {
        this.hdfsConfiguration = checkNotNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.fileSystemWrapper = checkNotNull(fileSystemWrapper, "fileSystemWrapper is null");
    }

    public Configuration getConfiguration(Path path)
    {
        String host = path.toUri().getHost();
        checkArgument(host != null, "path host is null: %s", path);
        return hdfsConfiguration.getConfiguration(host);
    }

    public FileSystemWrapper getFileSystemWrapper()
    {
        return fileSystemWrapper;
    }
}
