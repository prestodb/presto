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
package com.facebook.presto.orc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

public class TempFile
        implements Closeable
{
    private final File tempDir;
    private final File file;

    public TempFile()
    {
        tempDir = createTempDir();
        tempDir.mkdirs();
        file = new File(tempDir, "data.orc");
    }

    public File getFile()
    {
        return file;
    }

    @Override
    public void close()
            throws IOException
    {
        // hadoop creates crc files that must be deleted also, so just delete the whole directory
        deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
    }
}
