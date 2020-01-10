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

import java.util.Optional;

public class FileRepositoryConfig
{
    private String upstreamName = "upstream";
    private String originName = "origin";
    private Optional<String> directory = Optional.empty();
    private boolean checkDirectoryName = true;

    @NotNull
    public String getUpstreamName()
    {
        return upstreamName;
    }

    @Config("git.upstream-name")
    public FileRepositoryConfig setUpstreamName(String upstreamName)
    {
        this.upstreamName = upstreamName;
        return this;
    }

    @NotNull
    public String getOriginName()
    {
        return originName;
    }

    @Config("git.origin-name")
    public FileRepositoryConfig setOriginName(String originName)
    {
        this.originName = originName;
        return this;
    }

    @NotNull
    public Optional<String> getDirectory()
    {
        return directory;
    }

    @Config("git.directory")
    public FileRepositoryConfig setDirectory(String gitDirectory)
    {
        this.directory = Optional.ofNullable(gitDirectory);
        return this;
    }

    public boolean isCheckDirectoryName()
    {
        return checkDirectoryName;
    }

    @Config("git.check-directory-name")
    public FileRepositoryConfig setCheckDirectoryName(boolean checkDirectoryName)
    {
        this.checkDirectoryName = checkDirectoryName;
        return this;
    }
}
