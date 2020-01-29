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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.release.git.Git;
import com.facebook.presto.release.git.GitRepository;
import com.facebook.presto.release.maven.Maven;
import com.facebook.presto.release.maven.MavenVersion;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.asCharSource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class AbstractCutReleaseTask
        implements ReleaseTask
{
    private static final Logger log = Logger.get(AbstractCutReleaseTask.class);
    private final GitRepository repository;
    private final Git git;
    private final Maven maven;
    private final MavenVersion version;

    public AbstractCutReleaseTask(GitRepository repository, Git git, Maven maven)
    {
        this.repository = requireNonNull(repository, "repository is null");
        this.git = requireNonNull(git, "git is null");
        this.maven = requireNonNull(maven, "maven is null");
        this.version = requireNonNull(MavenVersion.fromDirectory(repository.getDirectory()), "version is null");
    }

    /**
     * Perform a custom update for the pom file.
     *
     * @param pom Content of the pom file in the root directory of the project.
     * @return Updated pom file content.
     */
    protected abstract String getUpdatedPom(String pom);

    @Override
    public void run()
    {
        try {
            cutRelease();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cutRelease()
            throws IOException
    {
        File pomFile = Paths.get(repository.getDirectory().getAbsolutePath(), "pom.xml").toFile();
        checkState(pomFile.exists(), "pom.xml does not exists: %s", pomFile.getAbsolutePath());
        checkState(!pomFile.isDirectory(), "pom.xml is not a file: %s", pomFile.getAbsolutePath());

        String pom = asCharSource(pomFile, UTF_8).read();
        String updatedPom = getUpdatedPom(pom);
        if (!pom.equals(updatedPom)) {
            asCharSink(pomFile, UTF_8).write(updatedPom);
        }

        String snapshotVersion = version.getNextVersion().getSnapshotVersion();
        maven.setVersions(snapshotVersion);
        git.add(".");
        git.commit(format("Prepare for next development iteration - %s", snapshotVersion));
        git.pushOrigin("master");

        String releaseBranch = "release-" + version.getVersion();
        git.checkout(Optional.of("HEAD~1"), Optional.of(releaseBranch));
        git.pushOrigin(releaseBranch);
        log.info("Release branch created: %s", releaseBranch);
    }
}
