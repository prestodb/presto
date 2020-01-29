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
import com.facebook.presto.release.maven.MavenModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import java.util.List;

@Command(name = "cut-release", description = "Cut new Presto release")
public class CutReleaseCommand
        extends AbstractReleaseCommand
{
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

    @Option(name = "--git-initialize-from-remote", title = "Initialize the local repository from remote")
    @ConfigProperty("presto.git.initialize-from-remote")
    public String gitInitializeFromRemote;

    @Option(name = "--upstream-repo", title = "Upstream repository name", description = "Format: <USER/ORGANIZATION>/<REPO>. i.e. prestodb/presto")
    @ConfigProperty("presto.git.upstream-repository")
    public String upstreamRepository;

    @Option(name = "--origin-repo", title = "Origin repository name", description = "Format: <USER/ORGANIZATION>/<REPO>. i.e. user/presto")
    @ConfigProperty("presto.git.origin-repository")
    public String originRepository;

    @Option(name = "--git-executable", title = "Git executable")
    @ConfigProperty("git.executable")
    public String gitExecutable;

    @Option(name = "--git-ssh-key-file", title = "Git SSH key file")
    @ConfigProperty("git.ssh-key-file")
    public String gitSshKeyFile;

    @Option(name = "--maven-executable", title = "Maven executable")
    @ConfigProperty("maven.executable")
    public String mavenExecutable;

    @Override
    protected List<Module> getModules()
    {
        return ImmutableList.of(
                new GitModule(),
                new GitRepositoryModule(ForPresto.class, "presto"),
                new MavenModule(ForPresto.class),
                new CutReleaseModule());
    }

    @Override
    protected Class<? extends ReleaseTask> getReleaseTask()
    {
        return CutReleaseTask.class;
    }
}
