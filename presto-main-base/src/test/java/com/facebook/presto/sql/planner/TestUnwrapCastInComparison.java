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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.lang.String.format;

public class TestUnwrapCastInComparison
        extends BasePlanTest
{
    @Test
    public void testEquals()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("A = SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '1.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32766'",
                anyTree(
                        filter("A = SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32766.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32767'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32767'",
                anyTree(
                        filter("A = SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32767.9'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32768'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testNotEquals()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1'",
                anyTree(
                        filter("A <> SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '1.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32766'",
                anyTree(
                        filter("A <> SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32766.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32767'",
                anyTree(
                        filter("A <> SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32767'",
                anyTree(
                        filter("A <> SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32767.9'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32768'",
                anyTree(
                        filter("A <> SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testLessThan()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1'",
                anyTree(
                        filter("A < SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1.1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '1.9'",
                anyTree(
                        filter("A < SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32766'",
                anyTree(
                        filter("A < SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32766.9'",
                anyTree(
                        filter("A < SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32767'",
                anyTree(
                        filter("A <> SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32767'",
                anyTree(
                        filter("A < SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32767.9'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32768'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testLessThanOrEqual()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1.1'",
                anyTree(
                        filter("A <= SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '1.9'",
                anyTree(
                        filter("A < SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32766'",
                anyTree(
                        filter("A <= SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32766.9'",
                anyTree(
                        filter("A < SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32767'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32767'",
                anyTree(
                        filter("A <= SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32767.9'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32768'",
                anyTree(
                        filter("A = SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= DOUBLE '-32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));
    }

    @Test
    public void testGreaterThan()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1.1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '1.9'",
                anyTree(
                        filter("A >= SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32766'",
                anyTree(
                        filter("A > SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32766.9'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32767'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32767'",
                anyTree(
                        filter("A > SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32767.9'",
                anyTree(
                        filter("A > SMALLINT '-32768'",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32768'",
                anyTree(
                        filter("A <> SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1'",
                anyTree(
                        filter("A >= SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1.1'",
                anyTree(
                        filter("A > SMALLINT '1'",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '1.9'",
                anyTree(
                        filter("A >= SMALLINT '2'",
                                values("A"))));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32766'",
                anyTree(
                        filter("A >= SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32766.9'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32767'",
                anyTree(
                        filter("A = SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '32768.1'",
                anyTree(
                        filter("A IS NULL AND NULL",
                                values("A"))));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32767'",
                anyTree(
                        filter("A >= SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32767.9'",
                anyTree(
                        filter("A > SMALLINT '-32768' ",
                                values("A"))));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32768'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= DOUBLE '-32768.1'",
                anyTree(
                        filter("NOT (A IS NULL) OR NULL",
                                values("A"))));
    }

    @Test
    public void testDistinctFrom()
    {
        // representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '1'",
                                values("A"))));

        // non-representable
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1.1'",
                output(
                        values("A")));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '1.9'",
                output(
                        values("A")));

        // below top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32766'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '32766'",
                                values("A"))));

        // round to top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32766.9'",
                output(
                        values("A")));

        // top of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32767'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '32767'",
                                values("A"))));

        // above range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '32768.1'",
                output(
                        values("A")));

        // above bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32767'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '-32767'",
                                values("A"))));

        // round to bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32767.9'",
                output(
                        values("A")));

        // bottom of range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32768'",
                anyTree(
                        filter("A IS DISTINCT FROM SMALLINT '-32768'",
                                values("A"))));

        // below range
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM DOUBLE '-32768.1'",
                output(
                        values("A")));
    }

    @Test
    public void testNull()
    {
        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <> CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a > CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a < CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a >= CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a <= CAST(NULL AS DOUBLE)",
                output(
                        filter("NULL",
                                values("A"))));

        assertPlan(
                "SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a IS DISTINCT FROM CAST(NULL AS DOUBLE)",
                output(
                        filter("NOT (CAST(A AS DOUBLE) IS NULL)",
                                values("A"))));
    }

    @Test
    public void smokeTests()
    {
        // smoke tests for various type combinations
        for (String type : Arrays.asList("SMALLINT", "INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES TINYINT '1') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = TINYINT '1'",
                                    values("A"))));
        }

        for (String type : Arrays.asList("INTEGER", "BIGINT", "REAL", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES SMALLINT '0') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = SMALLINT '1'",
                                    values("A"))));
        }

        for (String type : Arrays.asList("BIGINT", "DOUBLE")) {
            assertPlan(
                    format("SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = %s '1'", type),
                    anyTree(
                            filter("A = 1",
                                    values("A"))));
        }

        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("A = REAL '1.0'",
                                values("A"))));
    }

    @Test
    public void testTermOrder()
    {
        // ensure the optimization works when the terms of the comparison are reversed
        // vs the canonical <expr> <op> <literal> form
        assertPlan("SELECT * FROM (VALUES REAL '1') t(a) WHERE DOUBLE '1' = a",
                anyTree(
                        filter("A = REAL '1.0'",
                                values("A"))));
    }

    @Test
    public void testNoEffect()
    {
        // BIGINT->DOUBLE implicit cast is not injective
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = 1e0",
                                values("A"))));

        // BIGINT->REAL implicit cast is not injective
        assertPlan(
                "SELECT * FROM (VALUES BIGINT '1') t(a) WHERE a = REAL '1'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '1.0'",
                                values("A"))));

        // INTEGER->REAL implicit cast is not injective
        assertPlan(
                "SELECT * FROM (VALUES INTEGER '1') t(a) WHERE a = REAL '1'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '1.0'",
                                values("A"))));

        // DECIMAL(p)->DOUBLE not injective for p > 15
        assertPlan(
                "SELECT * FROM (VALUES CAST('1' AS DECIMAL(16))) t(a) WHERE a = DOUBLE '1'",
                anyTree(
                        filter("CAST(A AS DOUBLE) = 1E0",
                                values("A"))));

        // DECIMAL(p)->REAL not injective for p > 7
        assertPlan(
                "SELECT * FROM (VALUES CAST('1' AS DECIMAL(8))) t(a) WHERE a = REAL '1'",
                anyTree(
                        filter("CAST(A AS REAL) = REAL '1.0'",
                                values("A"))));

        // no implicit cast between VARCHAR->INTEGER
        assertPlan(
                "SELECT * FROM (VALUES VARCHAR '1') t(a) WHERE CAST(a AS INTEGER) = INTEGER '1'",
                anyTree(
                        filter("CAST(A AS INTEGER) = 1",
                                values("A"))));

        // no implicit cast between DOUBLE->INTEGER
        assertPlan(
                "SELECT * FROM (VALUES DOUBLE '1') t(a) WHERE CAST(a AS INTEGER) = INTEGER '1'",
                anyTree(
                        filter("CAST(A AS INTEGER) = 1",
                                values("A"))));
    }
}
