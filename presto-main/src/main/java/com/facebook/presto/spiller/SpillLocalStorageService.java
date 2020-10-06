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
package com.facebook.presto.spiller;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.spiller.SpillFileHolder;
import com.facebook.presto.spi.spiller.SpillStorageService;
import com.facebook.presto.spi.spiller.SpillStorageServiceProvider;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.Files.delete;

public class SpillLocalStorageService
        implements SpillStorageService
{
    private static final Logger log = Logger.get(SpillLocalStorageService.class);

    @Override
    public SpillFileHolder createTempFile(Path spillPath, String prefix, String suffix)
            throws IOException
    {
        return new SpillLocalFileHolder(Files.createTempFile(spillPath, prefix, suffix));
    }

    @Override
    public void createDirectories(Path path)
            throws IOException
    {
        Files.createDirectories(path);
    }

    @Override
    public boolean canWrite(Path path)
    {
        return path.toFile().canWrite();
    }

    @Override
    public double getAvailableSpacePercentage(Path path)
            throws IOException
    {
        FileStore fileStore = Files.getFileStore(path);
        return 1.0 * fileStore.getUsableSpace() / fileStore.getTotalSpace();
    }

    @Override
    public void removeFilesQuitely(Path path, String fileGlob)
    {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, fileGlob)) {
            stream.forEach(spillFile -> {
                try {
                    log.info("Deleting old spill file: " + spillFile);
                    delete(spillFile);
                }
                catch (Exception e) {
                    log.warn("Could not cleanup old spill file: " + spillFile);
                }
            });
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning spill files");
        }
    }

    public static class Provider
            implements SpillStorageServiceProvider
    {
        private static final SpillStorageService INSTANCE = new SpillLocalStorageService();

        @Override
        public String getName()
        {
            return "local";
        }

        @Override
        public SpillStorageService get()
        {
            return INSTANCE;
        }
    }
}
