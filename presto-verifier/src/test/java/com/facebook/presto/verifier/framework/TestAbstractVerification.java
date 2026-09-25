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
package com.facebook.presto.verifier.framework;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.verifier.framework.AbstractVerification.getDeterminismSkippedReason;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_DATA_CHANGED;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_INCONSISTENT_SCHEMA;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.ANALYSIS_FAILED_QUERY_FAILURE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.DETERMINISTIC;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_CATALOG;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_COLUMNS;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_LIMIT_CLAUSE;
import static com.facebook.presto.verifier.framework.DeterminismAnalysis.NON_DETERMINISTIC_ROW_COUNT;
import static com.facebook.presto.verifier.framework.SkippedReason.DETERMINISM_ANALYSIS_FAILED;
import static com.facebook.presto.verifier.framework.SkippedReason.NON_DETERMINISTIC;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;

public class TestAbstractVerification
{
    @DataProvider(name = "determinismSkippedReasons")
    public static Object[][] determinismSkippedReasons()
    {
        // Only a deterministic mismatch is a real failure; flaky and inconclusive results are skipped.
        return new Object[][] {
                {DETERMINISTIC, Optional.empty()},
                {NON_DETERMINISTIC_COLUMNS, Optional.of(NON_DETERMINISTIC)},
                {NON_DETERMINISTIC_ROW_COUNT, Optional.of(NON_DETERMINISTIC)},
                {NON_DETERMINISTIC_LIMIT_CLAUSE, Optional.of(NON_DETERMINISTIC)},
                {NON_DETERMINISTIC_CATALOG, Optional.of(NON_DETERMINISTIC)},
                {ANALYSIS_FAILED_INCONSISTENT_SCHEMA, Optional.of(DETERMINISM_ANALYSIS_FAILED)},
                {ANALYSIS_FAILED_QUERY_FAILURE, Optional.of(DETERMINISM_ANALYSIS_FAILED)},
                {ANALYSIS_FAILED_DATA_CHANGED, Optional.of(DETERMINISM_ANALYSIS_FAILED)},
                {ANALYSIS_FAILED, Optional.of(DETERMINISM_ANALYSIS_FAILED)},
        };
    }

    @Test(dataProvider = "determinismSkippedReasons")
    public void testDeterminismSkippedReason(DeterminismAnalysis analysis, Optional<SkippedReason> expected)
    {
        assertEquals(getDeterminismSkippedReason(Optional.of(analysis)), expected);
    }

    @Test
    public void testNoDeterminismAnalysisIsNotSkipped()
    {
        assertEquals(getDeterminismSkippedReason(Optional.empty()), Optional.empty());
    }

    @Test
    public void testEveryDeterminismAnalysisHasExpectedSkippedReason()
    {
        // Fails when a new DeterminismAnalysis value is added without an expected mapping above.
        Set<DeterminismAnalysis> covered = Arrays.stream(determinismSkippedReasons())
                .map(row -> (DeterminismAnalysis) row[0])
                .collect(toImmutableSet());
        assertEquals(covered, EnumSet.allOf(DeterminismAnalysis.class));
    }
}
