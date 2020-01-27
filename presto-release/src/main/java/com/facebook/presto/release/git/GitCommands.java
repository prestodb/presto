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

import com.facebook.presto.release.AbstractCommands;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class GitCommands
        extends AbstractCommands
        implements Git
{
    private final GitRepository repository;

    public GitCommands(GitRepository repository, GitConfig gitConfig)
    {
        super(
                gitConfig.getExecutable(),
                gitConfig.getSshKeyFile().map(s -> ImmutableMap.of("GIT_SSH_COMMAND", format("ssh -i %s", s))).orElseGet(ImmutableMap::of),
                repository.getDirectory());

        if (gitConfig.getSshKeyFile().isPresent()) {
            checkArgument(gitConfig.getSshKeyFile().get().exists(), "ssh key file does not exists: %s", gitConfig.getSshKeyFile().get().getAbsolutePath());
        }
        this.repository = requireNonNull(repository, "repository is null");
    }

    @Override
    public void add(String path)
    {
        command("add", path);
    }

    @Override
    public void checkout(Optional<String> ref, Optional<String> createBranch)
    {
        ImmutableList.Builder<String> arguments = ImmutableList.<String>builder().add("checkout");
        createBranch.ifPresent(branch -> arguments.add("-b").add(branch));
        ref.ifPresent(arguments::add);
        command(arguments.build());
    }

    @Override
    public void commit(String commitTitle)
    {
        command("commit", "-m", commitTitle);
    }

    @Override
    public void fastForwardUpstream(String ref)
    {
        command("pull", "--ff-only", repository.getUpstreamName(), ref);
    }

    @Override
    public void fetchUpstream(Optional<String> ref)
    {
        ImmutableList.Builder<String> arguments = ImmutableList.<String>builder()
                .add("fetch")
                .add(repository.getUpstreamName());
        ref.ifPresent(arguments::add);
        command(arguments.build());
    }

    @Override
    public String log(String revisionRange, String... options)
    {
        return command(ImmutableList.<String>builder()
                .add("log")
                .add(revisionRange)
                .addAll(asList(options))
                .build());
    }

    @Override
    public void pushOrigin(String branch)
    {
        command("push", repository.getOriginName(), "-u", branch + ":" + branch, "-f");
    }
}
