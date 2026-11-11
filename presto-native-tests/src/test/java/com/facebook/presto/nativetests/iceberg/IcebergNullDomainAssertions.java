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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.AllOrNoneValueSet;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.Map;
import java.util.function.Consumer;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.tests.QueryAssertions.assertQuery;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Asserts that an {@code IS NULL} or {@code IS NOT NULL} predicate on a column of a type that
 * is neither comparable nor orderable, such as {@code SphericalGeography} or {@code Geometry},
 * reaches the table scan as an {@link AllOrNoneValueSet} domain. The native worker turns that
 * domain into a Velox {@code IsNull} or {@code IsNotNull} filter in {@code toFilter}
 * (PrestoToVeloxConnectorUtils.cpp), so a query that passes this assertion and returns the
 * right rows exercises that conversion end to end, rather than a filter evaluated above the
 * scan.
 */
final class IcebergNullDomainAssertions
{
    private IcebergNullDomainAssertions() {}

    /**
     * Runs {@code actual} on the native runner and {@code expected} on the Java runner, and
     * asserts that the native plan's only table scan carries a null domain on {@code column}.
     */
    static void assertNullPredicateInScan(
            QueryRunner queryRunner,
            Session session,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            Session expectedSession,
            @Language("SQL") String expected,
            String column,
            boolean isNull)
    {
        assertQuery(queryRunner, session, actual, expectedQueryRunner, expectedSession, expected, false, false, scanHasNullDomain(column, isNull));
    }

    private static Consumer<Plan> scanHasNullDomain(String column, boolean isNull)
    {
        return plan -> {
            TableScanNode scan = searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findOnlyElement();
            assertTrue(scan.getTable().getLayout().isPresent(), "Table scan has no layout");
            IcebergTableLayoutHandle layout = (IcebergTableLayoutHandle) scan.getTable().getLayout().get();
            Map<Subfield, Domain> domains = layout.getDomainPredicate().getDomains()
                    .orElseThrow(() -> new AssertionError("Table scan domain predicate is none"));
            Domain domain = domains.get(new Subfield(column));
            assertNotNull(domain, format("No domain on column '%s' in table scan: %s", column, domains));
            assertTrue(domain.getValues() instanceof AllOrNoneValueSet, format("Domain on column '%s' is not an AllOrNoneValueSet: %s", column, domain));
            assertTrue(isNull ? domain.isOnlyNull() : domain.equals(Domain.notNull(domain.getType())), format("Domain on column '%s' is not %s: %s", column, isNull ? "IS NULL" : "IS NOT NULL", domain));
        };
    }
}
