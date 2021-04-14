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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

public class HdfsFileIO
        implements FileIO
{
    private final HdfsEnvironment environment;
    private final HdfsContext context;

    public HdfsFileIO(HdfsEnvironment environment, HdfsContext context)
    {
        this.environment = requireNonNull(environment, "environment is null");
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return new HdfsInputFile(new Path(path), environment, context);
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return new HdfsOutputFile(new Path(path), environment, context);
    }

    @Override
    public void deleteFile(String pathString)
    {
        Path path = new Path(pathString);
        try {
            environment.doAs(context.getIdentity().getUser(), () -> environment.getFileSystem(context, path).delete(path, false));
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to delete file: " + path, e);
        }
    }
}
