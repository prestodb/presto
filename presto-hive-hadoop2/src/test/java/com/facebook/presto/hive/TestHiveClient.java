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

import org.apache.hadoop.net.NetUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveClient
        extends AbstractTestHiveClient
{
    private int hiveVersionMajor;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.hiveVersionMajor",
            "hive.hadoop2.timeZone",
    })
    @BeforeClass
    public void initialize(String host, int port, String databaseName, int hiveVersionMajor, String timeZone)
            throws UnknownHostException
    {
        String hadoopMasterIp = System.getProperty("hadoop-master-ip");
        if (hadoopMasterIp != null) {
            // Even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
            // (e.g: in: NameNodeProxies.createProxy)
            // This adds a static resolution for hadoop-master to docker container internal ip
            NetUtils.addStaticResolution("hadoop-master", hadoopMasterIp);
        }

        setup(host, port, databaseName, timeZone);

        checkArgument(hiveVersionMajor > 0, "Invalid hiveVersionMajor: %s", hiveVersionMajor);
        this.hiveVersionMajor = hiveVersionMajor;
    }

    protected int getHiveVersionMajor()
    {
        checkState(hiveVersionMajor > 0, "hiveVersionMajor not set");
        return hiveVersionMajor;
    }

    @Override
    public void testGetPartitionSplitsTableOfflinePartition()
    {
        if (getHiveVersionMajor() >= 2) {
            throw new SkipException("ALTER TABLE .. ENABLE OFFLINE was removed in Hive 2.0 and this is a prerequisite for this test");
        }

        super.testGetPartitionSplitsTableOfflinePartition();
    }

    @Override
    public void testTypesRcBinary()
            throws Exception
    {
        if (getHiveVersionMajor() >= 3) {
            // TODO (https://github.com/prestosql/presto/issues/1218) requires https://issues.apache.org/jira/browse/HIVE-22167
            assertThatThrownBy(super::testTypesRcBinary)
                    .isInstanceOf(AssertionError.class)
                    .hasMessage("expected [2011-05-06 01:23:09.123] but found [2011-05-06 07:08:09.123]");
            return;
        }
        super.testTypesRcBinary();
    }

    @Override
    public void testTypesParquet()
            throws Exception
    {
        if (getHiveVersionMajor() >= 3) {
            // TODO (https://github.com/prestosql/presto/issues/1218) requires https://issues.apache.org/jira/browse/HIVE-21002
            assertThatThrownBy(super::testTypesParquet)
                    .isInstanceOf(AssertionError.class)
                    .hasMessage("expected [2011-05-06 01:23:09.123] but found [2011-05-06 07:08:09.123]");
            return;
        }
        super.testTypesParquet();
    }

    @Override
    public void testMismatchSchemaTable()
            throws Exception
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("Mismatch schema test fails in Hive 3 and above. Disabling it");
        }

        super.testMismatchSchemaTable();
    }
}
