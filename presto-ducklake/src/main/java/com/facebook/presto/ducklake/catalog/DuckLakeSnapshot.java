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
package com.facebook.presto.ducklake.catalog;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_snapshot}, joined with {@code ducklake_snapshot_changes} for the
 * author, commit message and changes-made summary.
 */
public class DuckLakeSnapshot
{
    private final long snapshotId;
    private final Instant snapshotTime;
    private final long schemaVersion;
    private final Optional<String> author;
    private final Optional<String> commitMessage;
    private final Optional<String> changesMade;

    public DuckLakeSnapshot(
            long snapshotId,
            Instant snapshotTime,
            long schemaVersion,
            Optional<String> author,
            Optional<String> commitMessage,
            Optional<String> changesMade)
    {
        this.snapshotId = snapshotId;
        this.snapshotTime = requireNonNull(snapshotTime, "snapshotTime is null");
        this.schemaVersion = schemaVersion;
        this.author = requireNonNull(author, "author is null");
        this.commitMessage = requireNonNull(commitMessage, "commitMessage is null");
        this.changesMade = requireNonNull(changesMade, "changesMade is null");
    }

    public long getSnapshotId()
    {
        return snapshotId;
    }

    public Instant getSnapshotTime()
    {
        return snapshotTime;
    }

    public long getSchemaVersion()
    {
        return schemaVersion;
    }

    public Optional<String> getAuthor()
    {
        return author;
    }

    public Optional<String> getCommitMessage()
    {
        return commitMessage;
    }

    public Optional<String> getChangesMade()
    {
        return changesMade;
    }
}
