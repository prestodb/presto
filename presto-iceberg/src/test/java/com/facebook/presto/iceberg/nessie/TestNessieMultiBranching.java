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
package com.facebook.presto.iceberg.nessie;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.containers.NessieContainer;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.nessie.NessieTestUtil.nessieConnectorProperties;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestNessieMultiBranching
        extends AbstractTestQueryFramework
{
    private NessieContainer nessieContainer;
    private NessieApiV1 nessieApiV1;

    @BeforeClass
    public void init()
            throws Exception
    {
        nessieContainer = NessieContainer.builder().build();
        nessieContainer.start();
        nessieApiV1 = HttpClientBuilder.builder().withUri(nessieContainer.getRestApiUri()).build(NessieApiV1.class);
        super.init();
    }

    @AfterClass
    public void tearDown()
    {
        if (nessieContainer != null) {
            nessieContainer.stop();
        }

        if (nessieApiV1 != null) {
            nessieApiV1.close();
        }
    }

    @AfterMethod
    public void resetData() throws NessieNotFoundException, NessieConflictException
    {
        Branch defaultBranch = nessieApiV1.getDefaultBranch();
        for (Reference r : nessieApiV1.getAllReferences().get().getReferences()) {
            if (r instanceof Branch && !r.getName().equals(defaultBranch.getName())) {
                nessieApiV1.deleteBranch().branch((Branch) r).delete();
            }
            if (r instanceof Tag) {
                nessieApiV1.deleteTag().tag((Tag) r).delete();
            }
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("iceberg_data");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .putAll(nessieConnectorProperties(nessieContainer.getRestApiUri()))
                .put("iceberg.file-format", new IcebergConfig().getFileFormat().name())
                .put("iceberg.catalog.warehouse", dataDirectory.getParent().toFile().toURI().toString())
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        queryRunner.execute("CREATE SCHEMA tpch");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());

        return queryRunner;
    }

    @Test
    public void testWithUnknownBranch()
    {
        assertQueryFails(sessionOnRef("unknownRef"), "CREATE SCHEMA nessie_namespace", ".*Nessie ref 'unknownRef' does not exist.*");
    }

    @Test
    public void testNamespaceVisibility()
            throws NessieConflictException, NessieNotFoundException
    {
        Reference one = createBranch("branchOne");
        Reference two = createBranch("branchTwo");
        Session sessionOne = sessionOnRef(one.getName());
        Session sessionTwo = sessionOnRef(two.getName());
        assertQuerySucceeds(sessionOne, "CREATE SCHEMA ns_one");
        assertThat(computeActual(sessionOne, "SHOW SCHEMAS FROM iceberg LIKE 'ns_one'").getMaterializedRows()).hasSize(1);
        assertQuerySucceeds(sessionTwo, "CREATE SCHEMA ns_two");
        assertThat(computeActual(sessionTwo, "SHOW SCHEMAS FROM iceberg LIKE 'ns_two'").getMaterializedRows()).hasSize(1);

        // ns_two shouldn't be visible on branchOne
        assertThat(computeActual(sessionOne, "SHOW SCHEMAS FROM iceberg LIKE 'ns_two'").getMaterializedRows()).isEmpty();
        // ns_one shouldn't be visible on branchTwo
        assertThat(computeActual(sessionTwo, "SHOW SCHEMAS FROM iceberg LIKE 'ns_one'").getMaterializedRows()).isEmpty();
    }

    @Test
    public void testTableDataVisibility()
            throws NessieConflictException, NessieNotFoundException
    {
        Reference ref = createBranch("tableDataVisibility");
        Session session = sessionOnRef(ref.getName());
        assertQuerySucceeds(session, "CREATE SCHEMA namespace_one");
        assertQuerySucceeds(session, "CREATE TABLE namespace_one.tbl (a int)");
        assertQuerySucceeds(session, "INSERT INTO namespace_one.tbl (a) VALUES (1)");
        assertQuerySucceeds(session, "INSERT INTO namespace_one.tbl (a) VALUES (2)");

        Reference one = createBranch("branchOneWithTable", ref.getName());
        Reference two = createBranch("branchTwoWithTable", ref.getName());
        Session sessionOne = sessionOnRef(one.getName());
        Session sessionTwo = sessionOnRef(two.getName());

        assertQuerySucceeds(sessionOne, "INSERT INTO namespace_one.tbl (a) VALUES (3)");

        assertQuerySucceeds(sessionTwo, "INSERT INTO namespace_one.tbl (a) VALUES (5)");
        assertQuerySucceeds(sessionTwo, "INSERT INTO namespace_one.tbl (a) VALUES (6)");

        // tableDataVisibility branch should still have 2 entries
        assertThat(computeScalar(session, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(2L);
        MaterializedResult rows = computeActual(session, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(2);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(getSession(), rows.getTypes()).row(1).row(2).build().getMaterializedRows());

        // there should be 3 entries on this branch
        assertThat(computeScalar(sessionOne, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(3L);
        rows = computeActual(sessionOne, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(3);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionOne, rows.getTypes()).row(1).row(2).row(3).build().getMaterializedRows());

        // and 4 entries on this branch
        assertThat(computeScalar(sessionTwo, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(4L);
        rows = computeActual(sessionTwo, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(4);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionTwo, rows.getTypes()).row(1).row(2).row(5).row(6).build().getMaterializedRows());

        // retrieve the second to the last commit hash and query the table with that hash
        List<LogResponse.LogEntry> logEntries = nessieApiV1.getCommitLog().refName(two.getName()).get().getLogEntries();
        assertThat(logEntries).isNotEmpty();
        String hash = logEntries.get(1).getCommitMeta().getHash();
        Session sessionTwoAtHash = sessionOnRef(two.getName(), hash);

        // at this hash there were only 3 rows
        assertThat(computeScalar(sessionTwoAtHash, "SELECT count(*) FROM namespace_one.tbl")).isEqualTo(3L);
        rows = computeActual(sessionTwoAtHash, "SELECT * FROM namespace_one.tbl");
        assertThat(rows.getMaterializedRows()).hasSize(3);
        assertEqualsIgnoreOrder(rows.getMaterializedRows(), resultBuilder(sessionTwoAtHash, rows.getTypes()).row(1).row(2).row(5).build().getMaterializedRows());
    }

    private Session sessionOnRef(String reference)
    {
        return Session.builder(getSession()).setCatalogSessionProperty("iceberg", "nessie_reference_name", reference).build();
    }

    private Session sessionOnRef(String reference, String hash)
    {
        return Session.builder(getSession()).setCatalogSessionProperty("iceberg", "nessie_reference_name", reference).setCatalogSessionProperty("iceberg", "nessie_reference_hash", hash).build();
    }

    private Reference createBranch(String branchName)
            throws NessieConflictException, NessieNotFoundException
    {
        return createBranch(branchName, "main");
    }

    private Reference createBranch(String branchName, String sourceBranch)
            throws NessieConflictException, NessieNotFoundException
    {
        Reference source = nessieApiV1.getReference().refName(sourceBranch).get();
        return nessieApiV1.createReference().sourceRefName(source.getName()).reference(Branch.of(branchName, source.getHash())).create();
    }
}
