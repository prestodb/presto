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
package com.facebook.presto.release.git;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

public class GitConfig
{
    private String executable = "git";
    private File sshKeyFile;

    @NotNull
    public String getExecutable()
    {
        return executable;
    }

    @Config("git.executable")
    public GitConfig setExecutable(String executable)
    {
        this.executable = executable;
        return this;
    }

    @NotNull
    public Optional<File> getSshKeyFile()
    {
        return Optional.ofNullable(sshKeyFile);
    }

    @Config("git.ssh-key-file")
    public GitConfig setSshKeyFile(File sshKeyFile)
    {
        this.sshKeyFile = sshKeyFile;
        return this;
    }
}
