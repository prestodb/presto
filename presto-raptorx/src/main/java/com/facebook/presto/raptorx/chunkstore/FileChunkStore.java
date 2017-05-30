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
package com.facebook.presto.raptorx.chunkstore;

import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_NOT_FOUND;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_STORE_ERROR;
import static com.facebook.presto.raptorx.storage.FileStorageService.getFileSystemPath;
import static java.nio.file.Files.deleteIfExists;
import static java.util.Objects.requireNonNull;

public class FileChunkStore
        implements ChunkStore
{
    private final File baseDir;

    @Inject
    public FileChunkStore(FileChunkStoreConfig config)
    {
        this(config.getDirectory());
    }

    public FileChunkStore(File baseDir)
    {
        this.baseDir = requireNonNull(baseDir, "baseDir is null");
    }

    @PostConstruct
    public void start()
    {
        createDirectories(baseDir);
    }

    @Override
    public void putChunk(long chunkId, File source)
    {
        File target = getChunkFile(chunkId);

        try {
            try {
                // Optimistically assume the file can be created
                copyFile(source, target);
            }
            catch (FileNotFoundException e) {
                createDirectories(target.getParentFile());
                copyFile(source, target);
            }
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_CHUNK_STORE_ERROR, "Chunk store put failed: " + chunkId, e);
        }
    }

    @Override
    public void getChunk(long chunkId, File target)
    {
        try {
            copyFile(getChunkFile(chunkId), target);
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(RAPTOR_CHUNK_NOT_FOUND, "Chunk not found in chunk store: " + chunkId, e);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_CHUNK_STORE_ERROR, "Chunk store get failed: " + chunkId, e);
        }
    }

    @Override
    public boolean deleteChunk(long chunkId)
    {
        try {
            return deleteIfExists(getChunkFile(chunkId).toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_CHUNK_STORE_ERROR, "Chunk store delete failed: " + chunkId, e);
        }
    }

    @Override
    public boolean chunkExists(long chunkId)
    {
        return getChunkFile(chunkId).isFile();
    }

    @VisibleForTesting
    public File getChunkFile(long chunkId)
    {
        return getFileSystemPath(baseDir, chunkId);
    }

    private static void createDirectories(File dir)
    {
        if (!dir.mkdirs() && !dir.isDirectory()) {
            throw new PrestoException(RAPTOR_CHUNK_STORE_ERROR, "Failed creating directories: " + dir);
        }
    }

    private static void copyFile(File source, File target)
            throws IOException
    {
        try (InputStream in = new FileInputStream(source);
                FileOutputStream out = new FileOutputStream(target)) {
            byte[] buffer = new byte[128 * 1024];
            while (true) {
                int n = in.read(buffer);
                if (n == -1) {
                    break;
                }
                out.write(buffer, 0, n);
            }
            out.flush();
            out.getFD().sync();
        }
    }
}
