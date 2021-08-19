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
package com.facebook.presto.hive.function.scalar;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.thrift.HiveMetastoreClientFactory;
import com.facebook.presto.hive.metastore.thrift.StaticHiveCluster;
import com.facebook.presto.hive.metastore.thrift.StaticMetastoreConfig;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;

public final class MaxDatePartitionFunction
{
    public static final String CONF_PATH = "/etc/catalog/hive.properties";
    public static final String PRESTO_HOME = "PRESTO_HOME";
    public static final String USER_NAME = "hive.metastore.user-name";
    public static final String URIS = "hive.metastore.uris";

    private static final Logger log = Logger.get(MaxDatePartitionFunction.class);
    private MaxDatePartitionFunction() {}
    private static StaticHiveCluster staticHiveCluster;

    @Description("return the max date partition of the given table")
    @ScalarFunction("max_pt")
    @SqlType("varchar")
    public static Slice maxDatePartition(@SqlType("varchar") Slice fullTableName) throws HiveException, TException, IOException, HiveMetaException
    {
        return maxDatePartition(fullTableName, Slices.utf8Slice("date"), Slices.utf8Slice("yyyyMMdd"));
    }

    @Description("return the max date partition of the given table")
    @ScalarFunction("max_pt")
    @SqlType("varchar")
    public static Slice maxDatePartition(@SqlType("varchar") Slice fullTableName,
                                         @SqlType("varchar") Slice partitionName,
                                         @SqlType("varchar") Slice dateFormat) throws HiveException, TException, IOException, HiveMetaException
    {
        checkArgument(fullTableName != null &&
                        (fullTableName.toStringUtf8().split("\\.").length == 2 || fullTableName.toStringUtf8().split("\\.").length == 3),
                "table name should be like [catalog.schema.table_name] or [schema.tabe_name]");

        String[] tables = fullTableName.toStringUtf8().split("\\.");
        String parttition = partitionName.toStringUtf8();
        String database = "";
        String table = "";
        if (tables.length == 3) {
            database = tables[0] + "." + tables[1];
            table = tables[2];
        }
        else if (tables.length == 2) {
            database = tables[0];
            table = tables[1];
        }

        getStaticHiveCluster();
        HiveMetastoreClient client = staticHiveCluster.createMetastoreClient(Optional.empty());

        String pt = "";
        int retryTimes = 5;
        while (retryTimes > 0) {
            retryTimes--;
            try {
                List<Partition> headPartition = client.listPartitions(database, table, (short) 10);
                List<Partition> partitions = new ArrayList<>();
                if (headPartition.size() == 0) {
                    return null;
                }
                else {
                    int x = 0;
                    while (partitions.size() == 0) {
                        partitions = client.listPartitionsByFilter(database, table,
                                buildFilter(parttition, dateFormat.toStringUtf8(), -x), (short) -1);
                        x++;
                    }
                }
                pt = partitions.stream().map(p -> p.getValues().get(0)).max(Comparator.naturalOrder()).get();
                break;
            }
            catch (TException e) {
                if (retryTimes == 0) {
                    log.error(e, "Failed to get partition information from table : %s", fullTableName.toStringUtf8());
                    throw new HiveMetaException(e);
                }
            }
            finally {
                client.close();
            }
        }
        return Slices.utf8Slice(pt);
    }

    private static String buildFilter(String partitionName, String dateFormat, int startMonth)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        SimpleDateFormat sdf2;
        if (dateFormat.indexOf('-') > 0) {
            sdf2 = new SimpleDateFormat("yyyyMMdd");
        }
        else {
            sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        }
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DATE, -15);
        c.add(Calendar.MONTH, startMonth);

        String startDate = sdf.format(c.getTime());
        String pStartDate = sdf2.format(c.getTime());

        c.add(Calendar.MONTH, 1);
        String endDate = sdf.format(c.getTime());
        String pEndDate = sdf2.format(c.getTime());

        return String.format("(%s >= '%s' and %s <= '%s') or (%s >= '%s' and %s <= '%s')",
                partitionName, startDate, partitionName, endDate,
                partitionName, pStartDate, partitionName, pEndDate);
    }

    private static void getStaticHiveCluster() throws HiveException, IOException
    {
        if (staticHiveCluster == null) {
            synchronized (MaxDatePartitionFunction.class) {
                if (staticHiveCluster == null) {
                    String prestoHome = System.getenv(PRESTO_HOME);
                    String confPath = prestoHome + CONF_PATH;
                    Properties props = new Properties();
                    try (FileInputStream fis = new FileInputStream(confPath);
                            BufferedInputStream bis = new BufferedInputStream(fis)) {
                        props.load(bis);
                    }
                    catch (FileNotFoundException e) {
                        log.error(e, "Failed to find config file : %s", confPath);
                        throw e;
                    }
                    String userName = props.getProperty(USER_NAME);
                    String uris = props.getProperty(URIS);
                    if (userName == null || uris == null) {
                        log.error("Failed to read userName or uris from %s", confPath);
                        throw new HiveException("Failed to read userName or uris from " + confPath);
                    }

                    StaticMetastoreConfig conf = new StaticMetastoreConfig();
                    conf.setMetastoreUsername(userName);
                    conf.setMetastoreUris(uris);

                    HiveMetastoreClientFactory hiveMetastoreClientFactory = new HiveMetastoreClientFactory(new MetastoreClientConfig(),
                            new NoHiveMetastoreAuthentication());
                    staticHiveCluster = new StaticHiveCluster(conf, hiveMetastoreClientFactory);
                }
            }
        }
    }
}
