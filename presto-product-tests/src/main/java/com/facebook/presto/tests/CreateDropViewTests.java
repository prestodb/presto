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

import com.facebook.presto.tests.ImmutableTpchTablesRequirements.ImmutableNationTable;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.tests.TestGroups.CREATE_DROP_VIEW;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ContextDsl.executeWith;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.query.QueryType.UPDATE;
import static com.teradata.tempto.sql.SqlContexts.createViewAs;
import static java.lang.String.format;

@Requires(ImmutableNationTable.class)
public class CreateDropViewTests
        extends ProductTest
{
    @Test(groups = CREATE_DROP_VIEW)
    public void createSimpleView()
            throws IOException
    {
        executeWith(createViewAs("SELECT * FROM nation"), view -> {
            assertThat(query(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void querySimpleViewQualified()
            throws IOException
    {
        executeWith(createViewAs("SELECT * FROM nation"), view -> {
            assertThat(query(format("SELECT %s.n_regionkey FROM %s", view.getName(), view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void createViewWithAggregate()
            throws IOException
    {
        executeWith(createViewAs("SELECT n_regionkey, count(*) countries FROM nation GROUP BY n_regionkey ORDER BY n_regionkey"), view -> {
            assertThat(query(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(5);
        });
    }

    @Test(groups = {CREATE_DROP_VIEW, SMOKE})
    public void createOrReplaceSimpleView()
            throws IOException
    {
        executeWith(createViewAs("SELECT * FROM nation"), view -> {
            assertThat(query(format("CREATE OR REPLACE VIEW %s AS SELECT * FROM nation", view.getName()), UPDATE))
                    .hasRowsCount(1);
            assertThat(query(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void createSimpleViewTwiceShouldFail()
            throws IOException
    {
        executeWith(createViewAs("SELECT * FROM nation"), view -> {
            assertThat(() -> query(format("CREATE VIEW %s AS SELECT * FROM nation", view.getName()), UPDATE))
                    .failsWithMessage("View already exists");
            assertThat(query(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = {CREATE_DROP_VIEW, SMOKE})
    public void dropViewTest()
            throws IOException
    {
        executeWith(createViewAs("SELECT * FROM nation"), view -> {
            assertThat(query(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
            assertThat(query(format("DROP VIEW %s", view.getName()), UPDATE))
                    .hasRowsCount(1);
            assertThat(() -> query(format("SELECT * FROM %s", view.getName())))
                    .failsWithMessage("does not exist");
        });
    }
}
