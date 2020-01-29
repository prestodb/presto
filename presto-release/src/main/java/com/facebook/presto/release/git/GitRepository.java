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

import com.google.common.collect.ImmutableList;

import javax.annotation.PostConstruct;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.release.AbstractCommands.command;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class GitRepository
{
    private static final Pattern REPOSITORY_PATTERN = Pattern.compile("\\w+/\\w+");

    private final String upstreamName;
    private final String originName;
    private final File directory;
    private final Optional<Runnable> initialization;

    private GitRepository(
            String upstreamName,
            String originName,
            File directory,
            Optional<Runnable> initialization)
    {
        this.upstreamName = requireNonNull(upstreamName, "upstreamName is null");
        this.originName = requireNonNull(originName, "originName is null");
        this.directory = requireNonNull(directory, "directory is null");
        this.initialization = requireNonNull(initialization, "initialization is null");
    }

    @PostConstruct
    public void initialize()
    {
        initialization.ifPresent(Runnable::run);
    }

    private static String getRemoteUrl(String repository)
    {
        checkArgument(REPOSITORY_PATTERN.matcher(repository).matches(), "Invalid repository name: %s, expect format <USER>/<REPO>");
        return format("git@github.com:%s.git", repository);
    }

    public static GitRepository fromFile(String repositoryName, FileRepositoryConfig fileConfig)
    {
        return new GitRepository(
                fileConfig.getUpstreamName(),
                fileConfig.getOriginName(),
                getValidatedDirectoryForFileRepository(fileConfig, repositoryName),
                Optional.empty());
    }

    public static GitRepository fromRemote(String repositoryName, FileRepositoryConfig fileConfig, RemoteRepositoryConfig remoteConfig, GitConfig gitConfig)
    {
        String upstreamUrl = getRemoteUrl(remoteConfig.getUpstreamRepository());
        String originUrl = getRemoteUrl(remoteConfig.getOriginRepository().orElse(remoteConfig.getUpstreamRepository()));
        File gitDirectory = getValidatedDirectoryForRemoteRepository(fileConfig, repositoryName);
        Map<String, String> environment = GitCommands.getEnvironment(gitConfig.getSshKeyFile());

        Runnable initialization = () -> {
            command(ImmutableList.of(gitConfig.getExecutable(), "clone", originUrl, gitDirectory.getAbsolutePath()), environment, new File(System.getProperty("user.dir")));
            command(ImmutableList.of(gitConfig.getExecutable(), "remote", "add", fileConfig.getUpstreamName(), upstreamUrl), environment, gitDirectory);
            if (!fileConfig.getOriginName().equals("origin")) {
                command(ImmutableList.of(gitConfig.getExecutable(), "remote", "add", fileConfig.getOriginName(), originUrl), environment, gitDirectory);
            }
        };

        return new GitRepository(
                fileConfig.getUpstreamName(),
                fileConfig.getOriginName(),
                gitDirectory,
                Optional.of(initialization));
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

    private static File getValidatedDirectoryForFileRepository(FileRepositoryConfig config, String repositoryName)
    {
        File gitDirectory = new File(config.getDirectory().orElse(System.getProperty("user.dir")));
        checkDirectoryName(config.isCheckDirectoryName(), gitDirectory, repositoryName);
        checkArgument(gitDirectory.exists(), "Does not exists: %s", gitDirectory.getAbsolutePath());
        checkArgument(gitDirectory.isDirectory(), "Not a directory: %s", gitDirectory.getAbsolutePath());
        return gitDirectory;
    }

    private static File getValidatedDirectoryForRemoteRepository(FileRepositoryConfig config, String repositoryName)
    {
        checkArgument(config.getDirectory().isPresent(), "Directory path is absent");
        File gitDirectory = new File(config.getDirectory().get());
        checkDirectoryName(config.isCheckDirectoryName(), gitDirectory, repositoryName);
        return gitDirectory;
    }

    private static void checkDirectoryName(boolean checkDirectoryName, File directory, String repositoryName)
    {
        if (checkDirectoryName) {
            checkArgument(directory.getName().equals(repositoryName), "Directory name [%s] mismatches repository name [%s]", directory.getName(), repositoryName);
        }
    }
}
