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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.release.ForPresto;
import com.facebook.presto.release.git.GitModule;
import com.facebook.presto.release.git.GitRepositoryModule;
import com.facebook.presto.release.git.GithubActionModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import static com.google.common.base.Throwables.throwIfUnchecked;

@Command(name = "release-notes", description = "Presto release notes generator")
public class GenerateReleaseNotesCommand
        implements Runnable
{
    private static final Logger log = Logger.get(GenerateReleaseNotesCommand.class);

    @Option(name = "--github-user", title = "Github username", required = true)
    public String githubUser;

    @Option(name = "--github-access-token", title = "Github Personal Access Token", required = true)
    public String githubAccessToken;

    @Option(name = "--version", title = "Release version")
    public String version;

    @Option(name = "--upstream-name", title = "Presto repo upstream name")
    public String gitUpstreamName;

    @Option(name = "--origin-name", title = "Presto repo origin name")
    public String gitOriginName;

    @Option(name = "--directory", title = "Presto repo directory")
    public String gitDirectory;

    @Option(name = "--check-directory-name", title = "Check whether the git directory name is presto")
    public String gitCheckDirectoryName;

    @Option(name = "--git-executable", title = "Git executable")
    public String gitExecutable;

    @Option(name = "--git-ssh-key-file", title = "Git SSH key file")
    public String gitSshKeyFile;

    @Override
    public void run()
    {
        System.setProperty("github.user", githubUser);
        System.setProperty("github.access-token", githubAccessToken);
        if (version != null) {
            System.setProperty("release-notes.version", version);
        }
        if (gitUpstreamName != null) {
            System.setProperty("presto.git.upstream-name", gitUpstreamName);
        }
        if (gitOriginName != null) {
            System.setProperty("presto.git.origin-name", gitOriginName);
        }
        if (gitDirectory != null) {
            System.setProperty("presto.git.directory", gitDirectory);
        }
        if (gitCheckDirectoryName != null) {
            System.setProperty("presto.git.check-directory-name", gitCheckDirectoryName);
        }
        if (gitExecutable != null) {
            System.setProperty("git.executable", gitExecutable);
        }
        if (gitSshKeyFile != null) {
            System.setProperty("git.ssh-key-file", gitSshKeyFile);
        }

        Bootstrap app = new Bootstrap(ImmutableList.of(
                new GitModule(),
                new GitRepositoryModule(ForPresto.class, "presto"),
                new GithubActionModule(),
                new GenerateReleaseNotesModule()));

        Injector injector = null;
        try {
            injector = app.strictConfig().initialize();
            injector.getInstance(GenerateReleaseNotesTask.class).run();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            if (injector != null) {
                try {
                    injector.getInstance(LifeCycleManager.class).stop();
                }
                catch (Exception e) {
                    log.error(e);
                }
            }
        }
    }
}
