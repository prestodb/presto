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
package com.facebook.presto.benchmark.driver;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.base.CharMatcher.anyOf;
import static com.google.common.base.Functions.forMap;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class BenchmarkResultsPrinter
        implements BenchmarkResultsStore
{
    private final List<String> tagNames;

    public BenchmarkResultsPrinter(Iterable<Suite> suites, Iterable<BenchmarkQuery> queries)
    {
        this(getSelectedQueryTagNames(suites, queries));
    }

    private static List<String> getSelectedQueryTagNames(Iterable<Suite> suites, Iterable<BenchmarkQuery> queries)
    {
        SortedSet<String> tags = new TreeSet<>();
        for (Suite suite : suites) {
            for (BenchmarkQuery query : suite.selectQueries(queries)) {
                tags.addAll(query.getTags().keySet());
            }

            for (RegexTemplate regexTemplate : suite.getSchemaNameTemplates()) {
                tags.addAll(regexTemplate.getFieldNames());
            }
        }
        return ImmutableList.copyOf(tags);
    }

    public BenchmarkResultsPrinter(List<String> tagNames)
    {
        this.tagNames = requireNonNull(tagNames, "tagNames is null");

        // print header row
        printRow(ImmutableList.builder()
                .add("suite")
                .add("query")
                .addAll(tagNames)
                .add("wallTimeP50")
                .add("wallTimeMean")
                .add("wallTimeStd")
                .add("processCpuTimeP50")
                .add("processCpuTimeMean")
                .add("processCpuTimeStd")
                .add("queryCpuTimeP50")
                .add("queryCpuTimeMean")
                .add("queryCpuTimeStd")
                .add("status")
                .add("error")
                .build());
    }

    @Override
    public void store(BenchmarkSchema benchmarkSchema, BenchmarkQueryResult result)
    {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.putAll(result.getBenchmarkQuery().getTags());
        tags.putAll(benchmarkSchema.getTags());

        // only print first line of error message
        Optional<String> errorMessage = result.getErrorMessage().map(error -> getFirst(Splitter.on(anyOf("\r\n")).trimResults().split(error), ""));

        printRow(ImmutableList.builder()
                .add(result.getSuite().getName())
                .add(result.getBenchmarkQuery().getName())
                .addAll(transform(tagNames, forMap(tags, "")))
                .add(NANOSECONDS.toMillis((long) result.getWallTimeNanos().getMedian()))
                .add(NANOSECONDS.toMillis((long) result.getWallTimeNanos().getMean()))
                .add(NANOSECONDS.toMillis((long) result.getWallTimeNanos().getStandardDeviation()))
                .add(NANOSECONDS.toMillis((long) result.getProcessCpuTimeNanos().getMedian()))
                .add(NANOSECONDS.toMillis((long) result.getProcessCpuTimeNanos().getMean()))
                .add(NANOSECONDS.toMillis((long) result.getProcessCpuTimeNanos().getStandardDeviation()))
                .add(NANOSECONDS.toMillis((long) result.getQueryCpuTimeNanos().getMedian()))
                .add(NANOSECONDS.toMillis((long) result.getQueryCpuTimeNanos().getMean()))
                .add(NANOSECONDS.toMillis((long) result.getQueryCpuTimeNanos().getStandardDeviation()))
                .add(result.getStatus().toString().toLowerCase(Locale.ENGLISH))
                .add(errorMessage.orElse(""))
                .build());
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void printRow(Iterable<?> values)
    {
        System.out.println(Joiner.on('\t').join(values));
    }
}
