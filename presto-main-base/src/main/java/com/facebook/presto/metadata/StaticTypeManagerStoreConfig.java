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
package com.facebook.presto.metadata;

import com.facebook.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.io.File;

public class StaticTypeManagerStoreConfig
{
    private File typeManagerConfigurationDir = new File("etc/type-managers/");

    @NotNull
    public File getTypeManagerConfigurationDir()
    {
        return typeManagerConfigurationDir;
    }

    @Config("type-manager.config-dir")
    public StaticTypeManagerStoreConfig setTypeManagerConfigurationDir(File dir)
    {
        this.typeManagerConfigurationDir = dir;
        return this;
    }
}
