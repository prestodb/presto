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

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GitRepository
{
    private final String upstreamName;
    private final String originName;
    private final File directory;

    private GitRepository(String upstreamName, String originName, File directory)
    {
        this.upstreamName = requireNonNull(upstreamName, "upstreamName is null");
        this.originName = requireNonNull(originName, "originName is null");
        this.directory = requireNonNull(directory, "directory is null");
    }

    public static GitRepository fromFile(String repositoryName, FileRepositoryConfig config)
    {
        return new GitRepository(
                config.getUpstreamName(),
                config.getOriginName(),
                validateGitDirectory(
                        config.getDirectory().orElse(System.getProperty("user.dir")),
                        repositoryName,
                        config.isCheckDirectoryName()));
    }

    public String getUpstreamName()
    {
        return upstreamName;
    }

    public String getOriginName()
    {
        return originName;
    }

    public File getDirectory()
    {
        return directory;
    }

    private static File validateGitDirectory(String gitDirectoryPath, String repositoryName, boolean checkGitDirectoryName)
    {
        File gitDirectory = new File(gitDirectoryPath);
        if (checkGitDirectoryName) {
            checkArgument(gitDirectory.getName().equals(repositoryName), "Directory name [%s] mismatches repository name [%s]", gitDirectory.getName(), repositoryName);
        }
        checkArgument(gitDirectory.exists(), "Does not exists: %s", gitDirectory.getAbsolutePath());
        checkArgument(gitDirectory.isDirectory(), "Not a directory: %s", gitDirectory.getAbsolutePath());
        return gitDirectory;
    }
}
