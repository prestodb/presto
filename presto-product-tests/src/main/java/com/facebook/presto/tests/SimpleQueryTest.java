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

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.assertions.QueryAssert;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.SIMPLE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.context.ThreadLocalTestContextHolder.testContextIfSet;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleQueryTest
        extends ProductTest
{
    private static class SimpleTestRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return new ImmutableTableRequirement(NATION);
        }
    }

    @BeforeTestWithContext
    public void beforeTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @AfterTestWithContext
    public void afterTest()
    {
        assertThat(testContextIfSet().isPresent()).isTrue();
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectAllFromNation()
    {
        QueryAssert.assertThat(query("select * from nation")).hasRowsCount(25);
    }

    @Test(groups = {SIMPLE, SMOKE})
    @Requires(SimpleTestRequirements.class)
    public void selectCountFromNation()
    {
        QueryAssert.assertThat(query("select count(*) from nation"))
                .hasRowsCount(1)
                .contains(row(25));
    }
}
