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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

class SyncingFileSystem
        extends RawLocalFileSystem
{
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
        private final FileOutputStream out;

        private LocalFileOutputStream(File file)
                throws IOException
        {
            this.out = new FileOutputStream(file);
        }

        @Override
        public void close()
                throws IOException
        {
            flush();
            out.getFD().sync();
            out.close();
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
        }

        @Override
        public void write(int b)
                throws IOException
        {
            out.write(b);
        }
    }
}
