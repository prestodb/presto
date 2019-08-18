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
package com.facebook.presto.raptor.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;
import java.nio.file.Files;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * The class overrides some inefficient methods from local file system
 */
public final class RaptorLocalFileSystem
        extends RawLocalFileSystem
{
    public RaptorLocalFileSystem(Configuration configuration)
            throws IOException
    {
        initialize(getUri(), configuration);
    }

    @Override
    public boolean exists(Path file)
    {
        return pathToFile(file).exists();
    }

    @Override
    public boolean rename(Path from, Path to)
            throws IOException
    {
        Files.move(pathToFile(from).toPath(), pathToFile(to).toPath(), ATOMIC_MOVE);
        return true;
    }
}
