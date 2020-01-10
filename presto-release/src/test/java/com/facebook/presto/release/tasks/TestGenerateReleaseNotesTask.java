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

import com.facebook.presto.release.git.Actor;
import com.facebook.presto.release.git.Commit;
import com.facebook.presto.release.git.FileRepositoryConfig;
import com.facebook.presto.release.git.GitActor;
import com.facebook.presto.release.git.GitRepository;
import com.facebook.presto.release.git.MockGit;
import com.facebook.presto.release.git.MockGithubAction;
import com.facebook.presto.release.git.PullRequest;
import com.facebook.presto.release.git.User;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.facebook.presto.release.tasks.GenerateReleaseNotesTask.RELEASE_NOTES_FILE;
import static com.facebook.presto.release.tasks.GenerateReleaseNotesTask.RELEASE_NOTES_LIST_FILE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.copy;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestGenerateReleaseNotesTask
{
    private static final String RESOURCE_DIRECTORY = "release-notes-test";
    private static final String VERSION = "0.231";
    private static final String COMMIT_HASH_PREFIX = Joiner.on("").join(nCopies(30, "a"));

    private static final Person USER1 = new Person("user1@gmail.com", "user1");
    private static final Person USER2 = new Person("user2@gmail.com", "user2");
    private static final Person USER3 = new Person("user3@gmail.com", "user3");

    private static final AtomicInteger PullRequestId = new AtomicInteger(1);
    private static final AtomicInteger commitId = new AtomicInteger(1);

    private static final List<Commit> COMMITS = ImmutableList.<Commit>builder()
            .addAll(createCommits("release notes 1", USER1, USER1, true, 1))
            .addAll(createCommits("release notes 2", USER2, USER1, true, 2))
            .addAll(createCommits("no release note", USER3, USER1, true, 1))
            .addAll(createCommits("no release notes", USER1, USER2, true, 2))
            .addAll(createCommits("missing release note", USER3, USER2, true, 2))
            .addAll(createCommits("missing section header 1", USER2, USER1, true, 1))
            .addAll(createCommits("missing section header 2", USER1, USER1, true, 1))
            .addAll(createCommits("missing section header 3", USER1, USER1, true, 1))
            .addAll(createCommits("missing asterisk", USER3, USER2, true, 1))
            .addAll(createCommits("disassociated commit", USER3, USER2, false, 1))
            .build();

    private File workingDirectory;
    private File releaseNotesListFile;
    private File releaseNotesFile;
    private MockGithubAction githubAction;

    @BeforeClass
    public void setup()
            throws IOException
    {
        this.workingDirectory = createTempDir();
        this.releaseNotesListFile = Paths.get(workingDirectory.getAbsolutePath(), RELEASE_NOTES_LIST_FILE).toFile();
        this.releaseNotesFile = Paths.get(workingDirectory.getAbsolutePath(), format(RELEASE_NOTES_FILE, VERSION)).toFile();

        checkState(releaseNotesListFile.getParentFile().mkdirs(), "Failed to create directory: %s", releaseNotesListFile.getParentFile());
        checkState(releaseNotesFile.getParentFile().mkdirs(), "Failed to create directory: %s", releaseNotesFile.getParentFile());
        copy(new File(getTestResource("release.rst").getFile()), releaseNotesListFile);
    }

    @AfterClass
    public void teardown()
            throws IOException
    {
        deleteRecursively(workingDirectory.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testGenerateReleaseNotes()
            throws Exception
    {
        GenerateReleaseNotesTask task = initializeTask(COMMITS);
        task.run();

        assertEquals(asCharSource(releaseNotesListFile, UTF_8).read(), getTestResourceContent("release_expected.rst"));
        assertEquals(asCharSource(releaseNotesFile, UTF_8).read(), getTestResourceContent("release-0.231_expected.rst"));
        assertEquals(githubAction.getCreatedPullRequest().getDescription(), getTestResourceContent("description_expected.txt"));
    }

    private GenerateReleaseNotesTask initializeTask(List<Commit> commits)
    {
        this.githubAction = new MockGithubAction(commits);
        return new GenerateReleaseNotesTask(
                GitRepository.fromFile(
                        workingDirectory.getName(),
                        new FileRepositoryConfig().setDirectory(workingDirectory.getAbsolutePath())),
                new MockGit(),
                githubAction,
                new GenerateReleaseNotesConfig().setVersion(VERSION));
    }

    private static PullRequest loadPullRequest(String title, Person author, Person mergedBy)
    {
        int id = PullRequestId.getAndIncrement();
        String fileName = title.replace(' ', '_') + ".txt";

        try {
            return new PullRequest(
                    id,
                    title,
                    format("https://github.com/prestodb/presto/pull/%s", id),
                    getTestResourceContent("pr", fileName),
                    author.getActor(),
                    mergedBy.getUser());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<Commit> createCommits(String title, Person author, Person mergedBy, boolean associateWithPullRequest, int commitCount)
    {
        List<PullRequest> associatedPullRequests = associateWithPullRequest ? ImmutableList.of(loadPullRequest(title, author, mergedBy)) : ImmutableList.of();

        return IntStream.range(0, commitCount).mapToObj(
                i -> new Commit(
                        format("%s%02d", COMMIT_HASH_PREFIX, commitId.getAndIncrement()),
                        author.getGitActor(),
                        format("%s - commit #%s", title, i + 1),
                        ImmutableMap.of("nodes", associatedPullRequests)))
                .collect(toImmutableList());
    }

    private static URL getTestResource(String... path)
    {
        return Resources.getResource(Paths.get(RESOURCE_DIRECTORY, path).toString());
    }

    private static String getTestResourceContent(String... path)
            throws IOException
    {
        return Resources.toString(getTestResource(path), UTF_8);
    }

    private static class Person
    {
        private final String login;
        private final String name;

        public Person(String login, String name)
        {
            this.login = requireNonNull(login, "login is null");
            this.name = requireNonNull(name, "name is null");
        }

        public Actor getActor()
        {
            return new Actor(login);
        }

        public User getUser()
        {
            return new User(name);
        }

        public GitActor getGitActor()
        {
            return new GitActor(name);
        }
    }
}
