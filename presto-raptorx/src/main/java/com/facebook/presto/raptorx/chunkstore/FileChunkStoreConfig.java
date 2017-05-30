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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.io.File;

public class FileChunkStoreConfig
{
    private File directory;

    @NotNull
    public File getDirectory()
    {
        return directory;
    }

    @Config("chunk-store.directory")
    @ConfigDescription("Base shared directory to use for the permanent copy of chunk data")
    public FileChunkStoreConfig setDirectory(File directory)
    {
        this.directory = directory;
        return this;
    }
}
