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
import com.facebook.presto.release.ForPresto;
import com.facebook.presto.release.git.Commit;
import com.facebook.presto.release.git.Git;
import com.facebook.presto.release.git.GitRepository;
import com.facebook.presto.release.git.GithubAction;
import com.facebook.presto.release.git.PullRequest;
import com.facebook.presto.release.maven.MavenVersion;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.facebook.presto.release.tasks.GenerateReleaseNotesTask.ExtractionStatus.EXPECT_DASHES_OR_RELEASE_NOTE;
import static com.facebook.presto.release.tasks.GenerateReleaseNotesTask.ExtractionStatus.EXPECT_LINE;
import static com.facebook.presto.release.tasks.GenerateReleaseNotesTask.ExtractionStatus.EXPECT_SECTION_HEADER;
import static com.google.common.base.Functions.identity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.asCharSource;
import static java.lang.Character.toUpperCase;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.Collections.sort;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;
import static java.util.stream.Collectors.joining;

public class GenerateReleaseNotesTask
        implements ReleaseTask
{
    private static final Logger log = Logger.get(GenerateReleaseNotesTask.class);

    public static final String RELEASE_NOTES_FILE = "presto-docs/src/main/sphinx/release/release-%s.rst";
    public static final String RELEASE_NOTES_LIST_FILE = "presto-docs/src/main/sphinx/release.rst";

    private static final Pattern IGNORED_COMMITS_PATTERN = Pattern.compile("\\[maven-release-plugin]|add release note(s)? for|prepare for next development iteration", CASE_INSENSITIVE);
    private static final Pattern NO_RELEASE_NOTE_PATTERN = Pattern.compile("== no release note(s)? ==", CASE_INSENSITIVE);
    private static final Pattern RELEASE_NOTE_PATTERN = Pattern.compile("== release note(s)? ==\\w*(.*)$", CASE_INSENSITIVE + MULTILINE + DOTALL);
    private static final Pattern HEADER_PATTERN = Pattern.compile("(.*) change(s)$", CASE_INSENSITIVE);
    private static final Pattern KNOWN_HEADER_PATTERN = Pattern.compile("(general|security|jdbc driver|web ui|.* connector|verifier|resource groups|spi)$", CASE_INSENSITIVE);
    private static final Pattern DASHES = Pattern.compile("-+$");

    private final GitRepository repository;
    private final Git git;
    private final GithubAction githubAction;
    private final MavenVersion version;

    @Inject
    public GenerateReleaseNotesTask(
            @ForPresto GitRepository repository,
            @ForPresto Git git,
            GithubAction githubAction,
            GenerateReleaseNotesConfig config)
    {
        this.repository = requireNonNull(repository, "repository is null");
        this.git = requireNonNull(git, "git is null");
        this.githubAction = requireNonNull(githubAction, "githubAction is null");
        this.version = requireNonNull(config.getVersion(), "version is null")
                .map(MavenVersion::fromReleaseVersion)
                .orElseGet(() -> MavenVersion.fromDirectory(repository.getDirectory()).getLastVersion());
    }

    @Override
    public void run()
    {
        git.checkout(Optional.of("master"), Optional.empty());
        git.fastForwardUpstream("master");
        git.fetchUpstream(Optional.empty());

        log.info("Release version: %s, Last Version: %s", version.getVersion(), version.getLastVersion().getVersion());
        List<String> commitIds = Splitter.on("\n")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(git.log(
                        format(
                                "%s/release-%s..%s/release-%s",
                                repository.getUpstreamName(),
                                version.getLastVersion().getVersion(),
                                repository.getUpstreamName(),
                                version.getVersion()),
                        "--format=%H",
                        "--date-order"));

        log.info("Fetching Github commits");
        List<Commit> commits = githubAction.listCommits("release-" + version.getVersion(), commitIds.get(commitIds.size() - 1));

        log.info("Fetched %s commits", commits.size());
        commits = commits.stream()
                .filter(commit -> !IGNORED_COMMITS_PATTERN.matcher(commit.getTitle()).find())
                .collect(toImmutableList());

        log.info("Processing %s commits", commits.size());
        List<PullRequest> pullRequests = commits.stream()
                .flatMap(commit -> commit.getAssociatedPullRequests().stream())
                .distinct()
                .collect(toImmutableList());
        Map<PullRequest, Optional<List<ReleaseNoteItem>>> releaseNoteItems = pullRequests.stream()
                .collect(toImmutableMap(identity(), this::extractReleaseNotes));

        log.info("Collecting author information");
        Map<String, String> userByLogin = new HashMap<>();
        for (Commit commit : commits) {
            for (PullRequest pullRequest : commit.getAssociatedPullRequests()) {
                userByLogin.putIfAbsent(pullRequest.getAuthorLogin(), commit.getAuthor());
            }
        }
        userByLogin = ImmutableMap.copyOf(userByLogin);

        log.info("Generating release notes");
        String releaseNotes = generateReleaseNotes(releaseNoteItems);
        String releaseNotesSummary = format(
                "%s\n%s\n%s",
                generateMissingReleaseNotes(releaseNoteItems, commits, userByLogin),
                generateExtractedReleaseNotes(releaseNoteItems, userByLogin),
                generateCommits(commits));

        log.info("Generating release notes pull request");
        String releaseNotesBranch = "release-notes-" + version.getVersion();
        createReleaseNotesCommit(version.getVersion(), releaseNotesBranch, releaseNotes);
        git.pushOrigin(releaseNotesBranch);

        PullRequest releaseNotesPullRequest = githubAction.createPullRequest(releaseNotesBranch, format("Add release notes for %s", version.getVersion()), releaseNotesSummary);
        log.info("Release notes pull request created: %s", releaseNotesPullRequest.getUrl());
    }

    enum ExtractionStatus
    {
        EXPECT_SECTION_HEADER,
        EXPECT_DASHES_OR_RELEASE_NOTE,
        EXPECT_LINE,
    }

    private Optional<List<ReleaseNoteItem>> extractReleaseNotes(PullRequest pullRequest)
    {
        if (NO_RELEASE_NOTE_PATTERN.matcher(pullRequest.getDescription()).find()) {
            return Optional.of(ImmutableList.of());
        }

        Matcher matcher = RELEASE_NOTE_PATTERN.matcher(pullRequest.getDescription() + "\n");
        if (!matcher.find()) {
            return Optional.empty();
        }

        // Use a state machine to extract release notes
        ImmutableList.Builder<ReleaseNoteItem> releaseNoteItems = ImmutableList.builder();
        String section = null;
        StringBuilder currentNote = null;
        ExtractionStatus status = EXPECT_SECTION_HEADER;

        for (String line : Splitter.on("\n").trimResults().split(matcher.group(2))) {
            switch (status) {
                case EXPECT_SECTION_HEADER:
                    if (line.isEmpty()) {
                        continue;
                    }
                    section = extractSection(line).orElse(null);
                    if (section == null) {
                        log.error(format("Bad release notes for PR #%s: expect section header, found [%s]", pullRequest.getId(), line));
                        return Optional.empty();
                    }
                    status = EXPECT_DASHES_OR_RELEASE_NOTE;
                    break;

                case EXPECT_DASHES_OR_RELEASE_NOTE:
                    if (DASHES.matcher(line).matches()) {
                        continue;
                    }
                    if (line.isEmpty()) {
                        log.error(format("Bad release notes for PR #%s: no release note for section [%s]", pullRequest.getId(), section));
                        return Optional.empty();
                    }
                    if (!line.startsWith("*")) {
                        log.error(format("Bad release notes for PR #%s at [%s]: release note starts without asterisk (*)", pullRequest.getId(), line));
                        return Optional.empty();
                    }
                    currentNote = new StringBuilder(line.substring(2).trim());
                    status = EXPECT_LINE;
                    break;

                case EXPECT_LINE:
                    if (line.isEmpty()) {
                        releaseNoteItems.add(new ReleaseNoteItem(section, currentNote.toString()));
                        status = EXPECT_SECTION_HEADER;
                    }
                    else if (line.startsWith("*")) {
                        releaseNoteItems.add(new ReleaseNoteItem(section, currentNote.toString()));
                        currentNote = new StringBuilder(line.substring(2).trim());
                    }
                    else {
                        Optional<String> possibleSection = extractSection(line);
                        if (possibleSection.isPresent()) {
                            releaseNoteItems.add(new ReleaseNoteItem(section, currentNote.toString()));
                            section = possibleSection.get();
                            status = EXPECT_DASHES_OR_RELEASE_NOTE;
                        }
                        else {
                            currentNote.append(" ").append(line);
                        }
                    }
                    break;
            }
        }
        return Optional.of(releaseNoteItems.build());
    }

    private Optional<String> extractSection(String line)
    {
        Matcher matcher = HEADER_PATTERN.matcher(line);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1).toLowerCase(ENGLISH));
        }

        matcher = KNOWN_HEADER_PATTERN.matcher(line);
        if (matcher.matches()) {
            return Optional.of(line);
        }

        return Optional.empty();
    }

    private String generateReleaseNotes(Map<PullRequest, Optional<List<ReleaseNoteItem>>> releaseNoteItems)
    {
        StringBuilder document = new StringBuilder(format("=============\nRelease %s\n=============\n\n", version.getVersion()));

        Multimap<String, ReleaseNoteItem> releaseNotesByCategory = releaseNoteItems.values().stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(List::stream)
                .collect(toImmutableListMultimap(ReleaseNoteItem::getSection, identity()));

        List<String> categories = new ArrayList<>(releaseNotesByCategory.keySet());
        categories.sort(new CategoryComparator());

        for (String category : categories) {
            String header = category + " Changes";
            document.append(header)
                    .append("\n")
                    .append(Joiner.on("").join(nCopies(header.length(), "_")));
            for (ReleaseNoteItem item : releaseNotesByCategory.get(category)) {
                document.append("\n").append(item.getFormatted("*", 0));
            }
            document.append("\n\n");
        }
        return document.toString().trim() + "\n";
    }

    private String generateMissingReleaseNotes(Map<PullRequest, Optional<List<ReleaseNoteItem>>> releaseNoteItems, List<Commit> commits, Map<String, String> userByLogin)
    {
        List<PullRequest> pullRequestsMissingReleaseNotes = releaseNoteItems.entrySet().stream()
                .filter(entry -> !entry.getValue().isPresent())
                .map(Map.Entry::getKey)
                .collect(toImmutableList());
        List<Commit> dissociatedCommits = commits.stream()
                .filter(commit -> commit.getAssociatedPullRequests().isEmpty())
                .collect(toImmutableList());

        Map<String, StringBuilder> missingByAuthor = new TreeMap<>();
        for (PullRequest pullRequest : pullRequestsMissingReleaseNotes) {
            String author = userByLogin.get(pullRequest.getAuthorLogin());
            missingByAuthor.putIfAbsent(author, new StringBuilder("## ").append(author).append("\n"));
            StringBuilder missing = missingByAuthor.get(author);
            missing.append("- [ ] ")
                    .append(pullRequest.getUrl())
                    .append(" ")
                    .append(pullRequest.getTitle());
            if (pullRequest.getMergedBy().isPresent()) {
                missing.append(" (Merged by: ")
                        .append(pullRequest.getMergedBy().get())
                        .append(")");
            }
            missing.append("\n");
        }
        for (Commit commit : dissociatedCommits) {
            String author = commit.getAuthor();
            missingByAuthor.putIfAbsent(author, new StringBuilder("## ").append(author).append("\n"));
            missingByAuthor.get(author)
                    .append("- [ ] ")
                    .append(commit.getId())
                    .append(" ")
                    .append(commit.getTitle())
                    .append("\n");
        }
        return "# Missing Release Notes\n" +
                missingByAuthor.values().stream()
                        .map(StringBuilder::toString)
                        .collect(joining("\n"));
    }

    private String generateExtractedReleaseNotes(Map<PullRequest, Optional<List<ReleaseNoteItem>>> releaseNoteItems, Map<String, String> userByLogin)
    {
        StringBuilder document = new StringBuilder("# Extracted Release Notes\n");

        List<PullRequest> pullRequests = new ArrayList<>(releaseNoteItems.keySet());
        sort(pullRequests, Comparator.comparing(PullRequest::getId));
        for (PullRequest pullRequest : pullRequests) {
            Optional<List<ReleaseNoteItem>> releaseNotes = releaseNoteItems.get(pullRequest);
            if (!releaseNotes.isPresent() || releaseNotes.get().isEmpty()) {
                continue;
            }

            document.append("- #")
                    .append(pullRequest.getId())
                    .append(" (Author: ")
                    .append(userByLogin.get(pullRequest.getAuthorLogin()))
                    .append("): ")
                    .append(pullRequest.getTitle())
                    .append("\n");
            for (ReleaseNoteItem item : releaseNotes.get()) {
                document.append(item.getFormatted("-", 2)).append("\n");
            }
        }
        return document.toString();
    }

    private String generateCommits(List<Commit> commits)
    {
        return "# All Commits\n" +
                commits.stream()
                        .map(commit -> format("- %s %s (%s)", commit.getId(), commit.getTitle(), commit.getAuthor()))
                        .collect(joining("\n"));
    }

    private void createReleaseNotesCommit(String version, String branch, String releaseNotes)
    {
        git.checkout(Optional.empty(), Optional.of(branch));

        try {
            String gitDirectory = repository.getDirectory().getAbsolutePath();
            asCharSink(Paths.get(gitDirectory, format(RELEASE_NOTES_FILE, version)).toFile(), UTF_8).write(releaseNotes);
            List<String> lines = new LinkedList<>(asCharSource(Paths.get(gitDirectory, RELEASE_NOTES_LIST_FILE).toFile(), UTF_8).readLines());
            lines.add(7, format("    release/release-%s", version));
            asCharSink(Paths.get(gitDirectory, RELEASE_NOTES_LIST_FILE).toFile(), UTF_8).write(Joiner.on("\n").join(lines) + "\n");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        git.add(".");
        git.commit(format("Add release notes for %s", version));
    }

    public static class CategoryComparator
            implements Comparator<String>
    {
        private static final List<Pattern> CATEGORY_ORDERS = ImmutableList.<Pattern>builder()
                .add(Pattern.compile("General"))
                .add(Pattern.compile("Security"))
                .add(Pattern.compile("JDBC Driver"))
                .add(Pattern.compile(".* Connector"))
                .add(Pattern.compile("Web UI"))
                .add(Pattern.compile("Verifier"))
                .add(Pattern.compile("Resource Groups"))
                .add(Pattern.compile("SPI"))
                .build();

        @Override
        public int compare(String category1, String category2)
        {
            int order1 = getCategoryOrder(category1);
            int order2 = getCategoryOrder(category2);
            if (order1 != order2) {
                return Integer.compare(order1, order2);
            }
            return category1.compareTo(category2);
        }

        private int getCategoryOrder(String category)
        {
            return IntStream.range(0, CATEGORY_ORDERS.size())
                    .filter(i -> CATEGORY_ORDERS.get(i).matcher(category).matches())
                    .findFirst()
                    .orElse(CATEGORY_ORDERS.size());
        }
    }

    private static class ReleaseNoteItem
    {
        private static final Set<String> ALL_UPPER_WORDS = ImmutableSet.of("jdbc", "spi", "ui");

        private final String section;
        private final String line;

        public ReleaseNoteItem(String section, String line)
        {
            this.section = formatCategory(requireNonNull(section, "section is null"));
            checkArgument(!Strings.isNullOrEmpty(line), "line is null or empty");
            this.line = toUpperCase(line.charAt(0)) + line.substring(1);
        }

        public String getSection()
        {
            return section;
        }

        public String getFormatted(String marking, int indent)
        {
            return format("%s%s %s%s", Joiner.on("").join(nCopies(indent, " ")), marking, line, line.endsWith(".") ? "" : ".");
        }

        private static String formatCategory(String category)
        {
            StringBuilder formatted = new StringBuilder();
            Scanner scanner = new Scanner(category);
            while (scanner.hasNext()) {
                String word = scanner.next();
                if (ALL_UPPER_WORDS.contains(word)) {
                    formatted.append(word.toUpperCase(ENGLISH));
                }
                else {
                    formatted.append(toUpperCase(word.charAt(0))).append(word.substring(1));
                }
                if (scanner.hasNext()) {
                    formatted.append(" ");
                }
            }
            return formatted.toString();
        }
    }
}
