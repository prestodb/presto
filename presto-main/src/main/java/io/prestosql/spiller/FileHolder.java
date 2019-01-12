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
package io.prestosql.spiller;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class FileHolder
        implements Closeable
{
    private final Path filePath;

    @GuardedBy("this")
    private boolean deleted;

    public FileHolder(Path filePath)
    {
        this.filePath = requireNonNull(filePath, "filePath is null");
    }

    public synchronized OutputStream newOutputStream(OpenOption... options)
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        return Files.newOutputStream(filePath, options);
    }

    public synchronized InputStream newInputStream(OpenOption... options)
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        return Files.newInputStream(filePath, options);
    }

    @Override
    public synchronized void close()
    {
        if (deleted) {
            return;
        }
        deleted = true;

        try {
            Files.delete(filePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
