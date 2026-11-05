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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.hudi.common.HudiColumnStatsPruner;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_CANDIDATE_BYTES;
import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_CANDIDATE_FILES;
import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_FALLBACKS;
import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_LOOKUP_TIME;
import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_SKIPPED_BYTES;
import static com.facebook.presto.common.RuntimeMetricName.HUDI_MDT_DATA_SKIPPING_SKIPPED_FILES;
import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hudi.common.HudiColumnStatsPruner.MAX_BATCH_SIZE;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

final class HudiPruningIterator
        extends AbstractIterator<HiveFileInfo>
{
    private static final Logger log = Logger.get(HudiPruningIterator.class);
    private final Iterator<HiveFileInfo> source;
    private final HudiColumnStatsPruner pruner;
    private final URI tableLocation;
    private final AtomicBoolean disabled;
    private final RuntimeStats runtimeStats;
    private Iterator<HiveFileInfo> retained = emptyIterator();

    HudiPruningIterator(
            Iterator<HiveFileInfo> source,
            HudiColumnStatsPruner pruner,
            String tablePath,
            AtomicBoolean disabled,
            RuntimeStats runtimeStats)
    {
        this.source = requireNonNull(source, "source is null");
        this.pruner = requireNonNull(pruner, "pruner is null");
        this.tableLocation = new Path(requireNonNull(tablePath, "tablePath is null")).toUri().normalize();
        this.disabled = requireNonNull(disabled, "disabled is null");
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    @Override
    protected HiveFileInfo computeNext()
    {
        while (!retained.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Hudi file pruning interrupted");
            }
            if (!source.hasNext()) {
                return endOfData();
            }
            List<HiveFileInfo> batch = new ArrayList<>(MAX_BATCH_SIZE);
            // Listing failures are not optional-index failures and must propagate.
            while (batch.size() < MAX_BATCH_SIZE && source.hasNext()) {
                batch.add(source.next());
            }
            runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_CANDIDATE_FILES, NONE, batch.size());
            runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_CANDIDATE_BYTES, BYTE, batch.stream().mapToLong(HiveFileInfo::getLength).sum());
            retained = filterBatch(batch).iterator();
        }
        return retained.next();
    }

    private List<HiveFileInfo> filterBatch(List<HiveFileInfo> batch)
    {
        if (disabled.get()) {
            return batch;
        }
        Map<HiveFileInfo, Pair<String, String>> keys = new HashMap<>();
        batch.forEach(file -> fileKey(file).ifPresent(key -> keys.put(file, key)));
        if (keys.isEmpty()) {
            return batch;
        }
        long start = System.nanoTime();
        try {
            Set<Pair<String, String>> retainedKeys = pruner.retainFiles(List.copyOf(keys.values()));
            List<HiveFileInfo> result = new ArrayList<>(batch.size());
            long skippedBytes = 0;
            for (HiveFileInfo file : batch) {
                if (!keys.containsKey(file) || retainedKeys.contains(keys.get(file))) {
                    result.add(file);
                }
                else {
                    skippedBytes += file.getLength();
                }
            }
            runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_SKIPPED_FILES, NONE, batch.size() - result.size());
            runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_SKIPPED_BYTES, BYTE, skippedBytes);
            return result;
        }
        catch (IOException | RuntimeException | LinkageError e) {
            // Missing optional MDT codecs or incompatible reader dependencies also
            // disable the optimization. VM failures such as OOM still propagate.
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException("Hudi file pruning interrupted");
            }
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                if (cause instanceof AccessControlException || cause instanceof AccessDeniedException || cause instanceof SecurityException ||
                        (cause instanceof PrestoException && ((PrestoException) cause).getErrorCode().equals(PERMISSION_DENIED.toErrorCode()))) {
                    throw new PrestoException(PERMISSION_DENIED, "Access denied reading Hudi column statistics", e);
                }
                if (cause instanceof CancellationException) {
                    throw (CancellationException) cause;
                }
                if (cause instanceof InterruptedException || (cause instanceof InterruptedIOException && !(cause instanceof SocketTimeoutException))) {
                    Thread.currentThread().interrupt();
                    throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Interrupted reading Hudi column statistics", e);
                }
            }
            if (disabled.compareAndSet(false, true)) {
                log.warn(e, "Hudi column statistics lookup failed; continuing without data skipping");
                runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_FALLBACKS, NONE, 1);
            }
            return batch;
        }
        finally {
            runtimeStats.addMetricValue(HUDI_MDT_DATA_SKIPPING_LOOKUP_TIME, NANO, System.nanoTime() - start);
        }
    }

    private Optional<Pair<String, String>> fileKey(HiveFileInfo file)
    {
        Path path = new Path(file.getPath());
        if (path.getParent() == null) {
            return Optional.empty();
        }
        URI parent = path.getParent().toUri().normalize();
        if (!path.getName().endsWith(".parquet") ||
                !Objects.equals(tableLocation.getScheme(), parent.getScheme()) ||
                !Objects.equals(tableLocation.getAuthority(), parent.getAuthority())) {
            return Optional.empty();
        }
        String base = tableLocation.getPath();
        if (parent.getPath().equals(base)) {
            return Optional.of(Pair.of("", path.getName()));
        }
        String prefix = base.endsWith("/") ? base : base + "/";
        if (!parent.getPath().startsWith(prefix)) {
            return Optional.empty();
        }
        return Optional.of(Pair.of(parent.getPath().substring(prefix.length()), path.getName()));
    }
}
