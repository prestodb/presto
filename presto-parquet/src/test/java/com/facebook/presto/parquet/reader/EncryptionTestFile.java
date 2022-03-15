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
package com.facebook.presto.parquet.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class EncryptionTestFile
{
    private final String fileName;
    private final SimpleGroup[] fileContent;

    public EncryptionTestFile(String fileName, SimpleGroup[] fileContent)
    {
        checkArgument(!isNullOrEmpty(fileName), "file name cannot be null or empty");
        this.fileName = fileName;
        checkArgument(fileContent != null && fileContent.length > 0, "file content cannot be null or empty");
        this.fileContent = fileContent;
    }

    public String getFileName()
    {
        return this.fileName;
    }

    public SimpleGroup[] getFileContent()
    {
        return fileContent;
    }

    public long getFileSize()
            throws IOException
    {
        Path path = new Path(fileName);
        return path.getFileSystem(new Configuration()).getFileStatus(path).getLen();
    }
}
