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
package com.facebook.presto.release.tasks;

import com.facebook.presto.release.ForPresto;
import com.facebook.presto.release.git.GitModule;
import com.facebook.presto.release.git.GitRepositoryModule;
import com.facebook.presto.release.git.GithubActionModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.List;

@Command(name = "release-notes", description = "Presto release notes generator")
public class GenerateReleaseNotesCommand
        extends AbstractReleaseCommand
{
    @Option(name = "--github-user", title = "Github username", required = true)
    @ConfigProperty("github.user")
    public String githubUser;

    @Option(name = "--github-access-token", title = "Github Personal Access Token", required = true)
    @ConfigProperty("github.access-token")
    public String githubAccessToken;

    @Option(name = "--version", title = "Release version")
    @ConfigProperty("release-notes.version")
    public String version;

    @Option(name = "--upstream-name", title = "Presto repo upstream name")
    @ConfigProperty("presto.git.upstream-name")
    public String gitUpstreamName;

    @Option(name = "--origin-name", title = "Presto repo origin name")
    @ConfigProperty("presto.git.origin-name")
    public String gitOriginName;

    @Option(name = "--directory", title = "Presto repo directory")
    @ConfigProperty("presto.git.directory")
    public String gitDirectory;

    @Option(name = "--check-directory-name", title = "Check whether the git directory name is presto")
    @ConfigProperty("presto.git.check-directory-name")
    public String gitCheckDirectoryName;

    @Option(name = "--git-executable", title = "Git executable")
    @ConfigProperty("git.executable")
    public String gitExecutable;

    @Option(name = "--git-ssh-key-file", title = "Git SSH key file")
    @ConfigProperty("git.ssh-key-file")
    public String gitSshKeyFile;

    @Override
    protected List<Module> getModules()
    {
        return ImmutableList.of(
                new GitModule(),
                new GitRepositoryModule(ForPresto.class, "presto"),
                new GithubActionModule(),
                new GenerateReleaseNotesModule());
    }

    @Override
    protected Class<? extends ReleaseTask> getReleaseTask()
    {
        return GenerateReleaseNotesTask.class;
    }
}
