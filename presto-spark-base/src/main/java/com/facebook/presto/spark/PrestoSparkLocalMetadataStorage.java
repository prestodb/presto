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
package com.facebook.presto.spark;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.Files.notExists;
import static java.nio.file.Files.readAllBytes;

public class PrestoSparkLocalMetadataStorage
        implements PrestoSparkMetadataStorage
{
    @Override
    public void write(String outputPath, byte[] data)
    {
        try {
            Path outputFile = Paths.get(outputPath);
            checkArgument(notExists(outputFile), "File already exist: %s", outputFile);
            Files.write(Paths.get(outputPath), data);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] read(String inputPath)
    {
        File file = new File(inputPath);
        checkArgument(file.exists() && file.isFile(), "file does not exist: %s", file);
        checkArgument(file.canRead(), "file is not readable: %s", file);
        try {
            return readAllBytes(Paths.get(inputPath));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
