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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;

import static com.facebook.presto.spiller.TempStorageSpillerConstants.SPILL_FILE_GLOB;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;

public class TempStorageSpillerUtil
{
    private static final Logger log = Logger.get(TempStorageSpillerUtil.class);

    private TempStorageSpillerUtil()
    {
    }

    public static void cleanupOldSpillFiles(Path path)
    {
        try (DirectoryStream<Path> stream = newDirectoryStream(path, SPILL_FILE_GLOB)) {
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
}
