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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsInputFile
        implements InputFile
{
    private final InputFile delegate;
    private final HdfsEnvironment environment;
    private final String user;

    public HdfsInputFile(Path path, HdfsEnvironment environment, HdfsContext context)
    {
        requireNonNull(path, "path is null");
        this.environment = requireNonNull(environment, "environment is null");
        requireNonNull(context, "context is null");
        try {
            this.delegate = HadoopInputFile.fromPath(path, environment.getFileSystem(context, path), environment.getConfiguration(context, path));
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to create input file: " + path, e);
        }
        this.user = context.getIdentity().getUser();
    }

    @Override
    public long getLength()
    {
        return environment.doAs(user, delegate::getLength);
    }

    @Override
    public SeekableInputStream newStream()
    {
        return environment.doAs(user, delegate::newStream);
    }

    @Override
    public String location()
    {
        return delegate.location();
    }

    @Override
    public boolean exists()
    {
        return environment.doAs(user, delegate::exists);
    }
}
