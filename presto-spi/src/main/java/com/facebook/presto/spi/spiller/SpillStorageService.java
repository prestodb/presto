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
package com.facebook.presto.spi.spiller;

import java.io.IOException;
import java.nio.file.Path;

public interface SpillStorageService
{
    int getSpillBufferSize();

    SpillFileHolder createTempFile(Path spillPath, String prefix, String suffix)
            throws IOException;

    void createDirectories(Path path)
            throws IOException;

    boolean canWrite(Path path);

    double getAvailableSpacePercentage(Path path)
            throws IOException;

    void removeFilesQuitely(Path path, String fileGlob);
}
