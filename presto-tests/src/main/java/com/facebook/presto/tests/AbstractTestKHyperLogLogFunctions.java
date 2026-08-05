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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

public abstract class AbstractTestKHyperLogLogFunctions
        extends AbstractTestQueryFramework
{
    @Test
    public void testMergeKHyperLogLog()
    {
        assertQueryWithSameQueryRunner("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1",
                "select k1, cardinality(khyperloglog_agg(v1, v2)), uniqueness_distribution(khyperloglog_agg(v1, v2)) from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), (2, 1, 11, 30), (2, 1, 11, 11), " +
                        "(2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1");

        assertQueryWithSameQueryRunner("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                        "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)",
                "select cardinality(khyperloglog_agg(v1, v2)), uniqueness_distribution(khyperloglog_agg(v1, v2)) from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), (2, 1, 11, 30), (2, 1, 11, 11), " +
                        "(2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2)");
    }

    @Test
    public void testKHyperLogLogScalarFunctions()
    {
        // khyperloglog_agg(x, y) summarizes x in a MinHash structure and the y values linked to each x in
        // per-x HyperLogLogs. The sketches below are exact (the number of distinct x is far below the
        // MinHash size), so the derived scalar functions are deterministic. KHyperLogLog is not a H2 type,
        // so both sides run on the same Presto query runner and the expected side is an algebraic identity.
        String sketchA = "select khyperloglog_agg(x, y) khll from (values (1, 10), (2, 20), (3, 30), (4, 40)) t(x, y)";
        String sketchB = "select khyperloglog_agg(x, y) khll from (values (3, 30), (4, 40), (5, 50), (6, 60)) t(x, y)";
        String sketchDisjoint = "select khyperloglog_agg(x, y) khll from (values (7, 70), (8, 80), (9, 90), (10, 100)) t(x, y)";

        // cardinality of the MinHash side is the number of distinct x.
        assertQueryWithSameQueryRunner(
                "select cardinality(khll) = 4 from (" + sketchA + ")",
                "select true");

        // intersection_cardinality counts the x values shared by both sketches: A and B share {3, 4}.
        assertQueryWithSameQueryRunner(
                "select intersection_cardinality(a.khll, b.khll) = 2 from (" + sketchA + ") a, (" + sketchB + ") b",
                "select true");
        // Self-intersection equals the cardinality; disjoint sketches intersect in nothing.
        assertQueryWithSameQueryRunner(
                "select intersection_cardinality(a.khll, a.khll) = 4 and intersection_cardinality(a.khll, c.khll) = 0 " +
                        "from (" + sketchA + ") a, (" + sketchDisjoint + ") c",
                "select true");

        // jaccard_index is 1 for identical sketches and 0 for disjoint ones.
        assertQueryWithSameQueryRunner(
                "select jaccard_index(a.khll, a.khll) = 1.0E0 and jaccard_index(a.khll, c.khll) = 0.0E0 " +
                        "from (" + sketchA + ") a, (" + sketchDisjoint + ") c",
                "select true");

        // merge_khll unions an array of sketches; the union of A and B has 6 distinct x ({1..6}).
        assertQueryWithSameQueryRunner(
                "select cardinality(merge_khll(array[a.khll, b.khll])) = 6 from (" + sketchA + ") a, (" + sketchB + ") b",
                "select true");
        // merge_khll of a null array returns null.
        assertQueryWithSameQueryRunner(
                "select cardinality(merge_khll(null)) is null",
                "select true");

        // Each x is linked to exactly one y, so every x has uniqueness 1 and the histogram puts all mass in bucket 1.
        assertQueryWithSameQueryRunner(
                "select uniqueness_distribution(khll)[1] = 1.0E0 from (" + sketchA + ")",
                "select true");
        // The one-argument form uses the MinHash size as the histogram size, matching the explicit form.
        assertQueryWithSameQueryRunner(
                "select uniqueness_distribution(khll) = uniqueness_distribution(khll, cardinality(khll)) from (" + sketchA + ")",
                "select true");
        // histogramSize controls the bucket count; uniqueness above it accumulates in the last bucket.
        assertQueryWithSameQueryRunner(
                "select cardinality(uniqueness_distribution(khll, 3)) = 3 from (" + sketchA + ")",
                "select true");

        // reidentification_potential is the fraction of x whose uniqueness is at or below the threshold.
        // Every x has uniqueness 1, so it is 1 at threshold 1 and 0 at threshold 0.
        assertQueryWithSameQueryRunner(
                "select reidentification_potential(khll, 1) = 1.0E0 and reidentification_potential(khll, 0) = 0.0E0 from (" + sketchA + ")",
                "select true");
    }
}
