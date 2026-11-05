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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.hudi.common.HudiColumnStatsConverter.isSupportedType;
import static com.facebook.presto.hudi.common.HudiStatisticsEvaluator.mayMatch;
import static java.util.Objects.requireNonNull;

public final class HudiColumnStatsPruner
{
    public static final int MAX_BATCH_SIZE = 128;
    private static final int MAX_COLUMNS = 32;

    private final TupleDomain<String> predicate;
    private final Map<String, Type> columns;
    private final HudiColumnStatsReader reader;

    public HudiColumnStatsPruner(TupleDomain<String> predicate, HudiColumnStatsReader reader)
    {
        requireNonNull(predicate, "predicate is null");
        this.reader = requireNonNull(reader, "reader is null");
        Map<String, Domain> supportedDomains = new HashMap<>();
        predicate.getDomains().orElse(Map.of()).forEach((name, domain) -> {
            if (isSupportedType(domain.getType())) {
                supportedDomains.put(name, domain);
            }
        });
        // Avoid fetching an unbounded number of metadata records for wide predicates.
        if (supportedDomains.size() > MAX_COLUMNS) {
            supportedDomains.clear();
        }
        this.predicate = predicate.isNone() ? TupleDomain.none() : TupleDomain.withColumnDomains(supportedDomains);
        Map<String, Type> types = new HashMap<>();
        supportedDomains.forEach((name, domain) -> types.put(name, domain.getType()));
        this.columns = Map.copyOf(types);
    }

    public boolean isUseful()
    {
        return !predicate.isAll();
    }

    /**
     * Returns retained file identities. Callers preserve their original file objects,
     * ordering and row filters. Lookup failures are propagated for the connector to
     * distinguish recoverable index failures from authorization and cancellation.
     */
    public Set<Pair<String, String>> retainFiles(List<Pair<String, String>> files)
            throws IOException
    {
        if (files.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("Too many files in a Hudi statistics batch");
        }
        if (predicate.isNone() || files.isEmpty()) {
            return Set.of();
        }
        if (predicate.isAll()) {
            return Set.copyOf(files);
        }
        Map<Pair<String, String>, Map<String, Domain>> statistics = reader.readColumnStats(List.copyOf(files), columns);
        Set<Pair<String, String>> retained = new HashSet<>();
        for (Pair<String, String> file : files) {
            if (mayMatch(predicate, statistics.getOrDefault(file, Map.of()))) {
                retained.add(file);
            }
        }
        return Set.copyOf(retained);
    }
}
