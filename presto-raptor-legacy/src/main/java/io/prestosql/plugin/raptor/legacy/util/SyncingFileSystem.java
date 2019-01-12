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
package io.prestosql.plugin.raptor.legacy.util;

import io.airlift.slice.XxHash64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

public final class SyncingFileSystem
        extends RawLocalFileSystem
{
    public SyncingFileSystem(Configuration configuration)
            throws IOException
    {
        initialize(getUri(), configuration);
    }

    @Override
    public FSDataOutputStream create(Path path, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        if (exists(path) && !overwrite) {
            throw new IOException("file already exists: " + path);
        }
        Path parent = path.getParent();
        if ((parent != null) && !mkdirs(parent)) {
            throw new IOException("mkdirs failed to create " + parent.toString());
        }
        return new FSDataOutputStream(
                new BufferedOutputStream(new LocalFileOutputStream(pathToFile(path)), bufferSize),
                statistics);
    }

    private static class LocalFileOutputStream
            extends OutputStream
    {
        private final byte[] oneByte = new byte[1];
        private final XxHash64 hash = new XxHash64();
        private final File file;
        private final FileOutputStream out;
        private boolean closed;

        private LocalFileOutputStream(File file)
                throws IOException
        {
            this.file = requireNonNull(file, "file is null");
            this.out = new FileOutputStream(file);
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;

            flush();
            out.getFD().sync();
            out.close();

            // extremely paranoid code to detect a broken local file system
            try (InputStream in = new FileInputStream(file)) {
                if (hash.hash() != XxHash64.hash(in)) {
                    throw new IOException("File is corrupt after write");
                }
            }
        }

        @Override
        public void flush()
                throws IOException
        {
            out.flush();
        }

        @Override
        public void write(byte[] b, int off, int len)
                throws IOException
        {
            out.write(b, off, len);
            hash.update(b, off, len);
        }

        @SuppressWarnings("NumericCastThatLosesPrecision")
        @Override
        public void write(int b)
                throws IOException
        {
            oneByte[0] = (byte) (b & 0xFF);
            write(oneByte, 0, 1);
        }
    }
}
