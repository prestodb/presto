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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCallCenter;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCatalogPage;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCatalogReturns;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCatalogSales;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCustomerAddress;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsCustomerDemographics;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsDateDim;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsHouseholdDemographics;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsIncomeBand;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsInventory;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsItem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsPromotion;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsReason;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsShipMode;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsStore;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsStoreReturns;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsStoreSales;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsTimeDim;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsWarehouse;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsWebPage;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsWebReturns;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsWebSales;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createTpcdsWebSite;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractTestNativeTpcdsQueries
        extends AbstractTestQueryFramework
{
    private final String storageFormat;
    private final Boolean useThrift;

    public AbstractTestNativeTpcdsQueries(boolean useThrift, String storageFormat)
    {
        this.useThrift = useThrift;
        this.storageFormat = storageFormat;
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(
                Optional.of(Paths.get(dataDirectory)), "sql-standard", storageFormat, false);

        Session session = getSession();
        createTpcdsCallCenter(queryRunner, session);
        createTpcdsCatalogPage(queryRunner, session);
        createTpcdsCatalogReturns(queryRunner, session);
        createTpcdsCatalogSales(queryRunner, session);
        createTpcdsCustomer(queryRunner, session);
        createTpcdsCustomerAddress(queryRunner, session);
        createTpcdsCustomerDemographics(queryRunner, session);
        createTpcdsDateDim(queryRunner, session);
        createTpcdsHouseholdDemographics(queryRunner, session);
        createTpcdsIncomeBand(queryRunner, session);
        createTpcdsInventory(queryRunner, session);
        createTpcdsItem(queryRunner, session);
        createTpcdsPromotion(queryRunner, session);
        createTpcdsReason(queryRunner, session);
        createTpcdsShipMode(queryRunner, session);
        createTpcdsStore(queryRunner, session);
        createTpcdsStoreReturns(queryRunner, session);
        createTpcdsStoreSales(queryRunner, session);
        createTpcdsTimeDim(queryRunner, session);
        createTpcdsWarehouse(queryRunner, session);
        createTpcdsWebPage(queryRunner, session);
        createTpcdsWebReturns(queryRunner, session);
        createTpcdsWebSales(queryRunner, session);
        createTpcdsWebSite(queryRunner, session);

        return queryRunner;
    }

    protected static String getTpcdsQuery(String q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpcds/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceFirst("(?m);$", "");
        return sql;
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession()).setSchema("tpcds").build();
    }

    @Test
    public void testTpcdsQ1()
            throws Exception
    {
        assertQuery(getTpcdsQuery("01"));
    }

    @Test
    public void testTpcdsQ2()
            throws Exception
    {
        assertQuery(getTpcdsQuery("02"));
    }

    @Test
    public void testTpcdsQ3()
            throws Exception
    {
        assertQuery(getTpcdsQuery("03"));
    }

    @Test
    public void testTpcdsQ4()
            throws Exception
    {
        assertQuery(getTpcdsQuery("04"));
    }

    @Test
    public void testTpcdsQ5()
            throws Exception
    {
        assertQuery(getTpcdsQuery("05"));
    }

    @Test
    public void testTpcdsQ6()
            throws Exception
    {
        assertQuery(getTpcdsQuery("06"));
    }

    @Test
    public void testTpcdsQ7()
            throws Exception
    {
        assertQuery(getTpcdsQuery("07"));
    }

    @Test
    public void testTpcdsQ8()
            throws Exception
    {
        assertQuery(getTpcdsQuery("08"));
    }

    @Test
    public void testTpcdsQ9()
            throws Exception
    {
        assertQuery(getTpcdsQuery("09"));
    }

    @Test
    public void testTpcdsQ10()
            throws Exception
    {
        assertQuery(getTpcdsQuery("10"));
    }

    @Test
    public void testTpcdsQ11()
            throws Exception
    {
        assertQuery(getTpcdsQuery("11"));
    }

    @Test(enabled = false)
    public void testTpcdsQ12()
            throws Exception
    {
        assertQuery(getTpcdsQuery("12"));
    }

    @Test
    public void testTpcdsQ13()
            throws Exception
    {
        assertQuery(getTpcdsQuery("13"));
    }

    @Test
    public void testTpcdsQ14_1()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("14_1"),
                "[\\s\\S]*Unexpected parameters[\\s\\S]*for function sum[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ14_2()
            throws Exception
    {
        assertQuery(getTpcdsQuery("14_2"));
    }

    @Test
    public void testTpcdsQ15()
            throws Exception
    {
        assertQuery(getTpcdsQuery("15"));
    }

    @Test
    public void testTpcdsQ16()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("16"), "[\\s\\S]*mismatched input \'\"total shipping cost\"\'[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ17()
            throws Exception
    {
        assertQuery(getTpcdsQuery("17"));
    }

    @Test
    public void testTpcdsQ18()
            throws Exception
    {
        // Results not equal
        assertQuerySucceeds(getTpcdsQuery("18"));
    }

    @Test
    public void testTpcdsQ19()
            throws Exception
    {
        assertQuery(getTpcdsQuery("19"));
    }

    // Disabled this case since it might crash the worker
    @Test(enabled = false)
    public void testTpcdsQ20()
            throws Exception
    {
        assertQuery(getTpcdsQuery("20"));
    }

    @Test
    public void testTpcdsQ21()
            throws Exception
    {
        assertQuery(getTpcdsQuery("21"));
    }

    @Test
    public void testTpcdsQ22()
            throws Exception
    {
        assertQuery(getTpcdsQuery("22"));
    }

    @Test
    public void testTpcdsQ23_1()
            throws Exception
    {
        assertQuery(getTpcdsQuery("23_1"));
    }

    @Test
    public void testTpcdsQ23_2()
            throws Exception
    {
        assertQuery(getTpcdsQuery("23_2"));
    }

    @Test
    public void testTpcdsQ24_1()
            throws Exception
    {
        assertQuery(getTpcdsQuery("24_1"));
    }

    @Test
    public void testTpcdsQ24_2()
            throws Exception
    {
        assertQuery(getTpcdsQuery("24_2"));
    }

    @Test
    public void testTpcdsQ25()
            throws Exception
    {
        assertQuery(getTpcdsQuery("25"));
    }

    @Test
    public void testTpcdsQ26()
            throws Exception
    {
        assertQuery(getTpcdsQuery("26"));
    }

    @Test
    public void testTpcdsQ27()
            throws Exception

    {
        // Results not equal
        assertQuerySucceeds(getTpcdsQuery("27"));
    }

    @Test
    public void testTpcdsQ28()
            throws Exception
    {
        // Results not equal
        // Actual rows (up to 100 of 1 extra rows shown, 1 rows in total):
        //    [77.93, 1468, 1468, 69.55, 1518, 1518, 134.06, 1167, 1167, 81.56, 1258, 1258, 60.27, 1523, 1523, 38.99, 1322, 1322]
        //Expected rows (up to 100 of 1 missing rows shown, 1 rows in total):
        //    [77.93, 1468, 1345, 69.55, 1518, 1331, 134.06, 1167, 1107, 81.56, 1258, 1158, 60.27, 1523, 1342, 38.99, 1322, 1152]
        assertQuerySucceeds(getTpcdsQuery("28"));
    }

    @Test
    public void testTpcdsQ29()
            throws Exception
    {
        assertQuery(getTpcdsQuery("29"));
    }

    @Test
    public void testTpcdsQ30()
            throws Exception
    {
        assertQuery(getTpcdsQuery("30"));
    }

    @Test
    public void testTpcdsQ31()
            throws Exception
    {
        assertQuery(getTpcdsQuery("31"));
    }

    // Disabled this case since it might crash the worker
    @Test(enabled = false)
    public void testTpcdsQ32()
            throws Exception
    {
        assertQuery(getTpcdsQuery("32"));
    }

    @Test
    public void testTpcdsQ33()
            throws Exception
    {
        assertQuery(getTpcdsQuery("33"));
    }

    @Test
    public void testTpcdsQ34()
            throws Exception
    {
        assertQuery(getTpcdsQuery("34"));
    }

    @Test
    public void testTpcdsQ35()
            throws Exception
    {
        assertQuery(getTpcdsQuery("35"));
    }

    @Test
    public void testTpcdsQ36()
            throws Exception
    {
        assertQuery(getTpcdsQuery("36"));
    }

    @Test
    public void testTpcdsQ37()
            throws Exception
    {
        assertQuery(getTpcdsQuery("37"));
    }

    @Test
    public void testTpcdsQ38()
            throws Exception
    {
        assertQuery(getTpcdsQuery("38"));
    }

    @Test
    public void testTpcdsQ39_1()
            throws Exception
    {
        assertQuery(getTpcdsQuery("39_1"));
    }

    @Test
    public void testTpcdsQ39_2()
            throws Exception
    {
        assertQuery(getTpcdsQuery("39_2"));
    }

    @Test
    public void testTpcdsQ40()
            throws Exception
    {
        assertQuery(getTpcdsQuery("40"));
    }

    @Test
    public void testTpcdsQ41()
            throws Exception
    {
        assertQuery(getTpcdsQuery("41"));
    }

    @Test
    public void testTpcdsQ42()
            throws Exception
    {
        assertQuery(getTpcdsQuery("42"));
    }

    @Test
    public void testTpcdsQ43()
            throws Exception
    {
        assertQuery(getTpcdsQuery("43"));
    }

    @Test
    public void testTpcdsQ44()
            throws Exception
    {
        assertQuery(getTpcdsQuery("44"));
    }

    @Test
    public void testTpcdsQ45()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("45"), "([\\s\\S]*Field not found:[\\s\\S]*)|([\\s\\S]*Exhausted retries:[\\s\\S]*)|([\\s\\S]*Unsupported Filter over SemiJoin[\\s\\S]*)");
    }

    @Test
    public void testTpcdsQ46()
            throws Exception
    {
        assertQuery(getTpcdsQuery("46"));
    }

    @Test
    public void testTpcdsQ47()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("47"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ48()
            throws Exception
    {
        assertQuery(getTpcdsQuery("48"));
    }

    @Test
    public void testTpcdsQ49()
            throws Exception
    {
        assertQuery(getTpcdsQuery("49"));
    }

    @Test
    public void testTpcdsQ50()
            throws Exception
    {
        assertQuery(getTpcdsQuery("50"));
    }

    @Test
    public void testTpcdsQ51()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("51"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ52()
            throws Exception
    {
        assertQuery(getTpcdsQuery("52"));
    }

    @Test
    public void testTpcdsQ53()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("53"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ54()
            throws Exception
    {
        assertQuery(getTpcdsQuery("54"));
    }

    @Test
    public void testTpcdsQ55()
            throws Exception
    {
        assertQuery(getTpcdsQuery("55"));
    }

    @Test
    public void testTpcdsQ56()
            throws Exception
    {
        assertQuery(getTpcdsQuery("56"));
    }

    @Test
    public void testTpcdsQ57()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("57"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ58()
            throws Exception
    {
        assertQuery(getTpcdsQuery("58"));
    }

    @Test
    public void testTpcdsQ59()
            throws Exception
    {
        assertQuery(getTpcdsQuery("59"));
    }

    @Test
    public void testTpcdsQ60()
            throws Exception
    {
        assertQuery(getTpcdsQuery("60"));
    }

    @Test
    public void testTpcdsQ61()
            throws Exception
    {
        assertQuery(getTpcdsQuery("61"));
    }

    @Test
    public void testTpcdsQ62()
            throws Exception
    {
        assertQuery(getTpcdsQuery("62"));
    }

    @Test
    public void testTpcdsQ63()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("63"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ64()
            throws Exception
    {
        assertQuery(getTpcdsQuery("64"));
    }

    @Test
    public void testTpcdsQ65()
            throws Exception
    {
        assertQuery(getTpcdsQuery("65"));
    }

    @Test
    public void testTpcdsQ66()
            throws Exception
    {
        assertQuery(getTpcdsQuery("66"));
    }

    @Test
    public void testTpcdsQ67()
            throws Exception
    {
        assertQuery(getTpcdsQuery("67"));
    }

    @Test
    public void testTpcdsQ68()
            throws Exception
    {
        assertQuery(getTpcdsQuery("68"));
    }

    @Test
    public void testTpcdsQ69()
            throws Exception
    {
        assertQuerySucceeds(getTpcdsQuery("69"));
    }

    @Test
    public void testTpcdsQ70()
            throws Exception
    {
        assertQuery(getTpcdsQuery("70"));
    }

    @Test
    public void testTpcdsQ71()
            throws Exception
    {
        assertQuery(getTpcdsQuery("71"));
    }

    @Test
    public void testTpcdsQ72()
            throws Exception
    {
        assertQuery(getTpcdsQuery("72"));
    }

    @Test
    public void testTpcdsQ73()
            throws Exception
    {
        assertQuery(getTpcdsQuery("73"));
    }

    @Test
    public void testTpcdsQ74()
            throws Exception
    {
        assertQuery(getTpcdsQuery("74"));
    }

    @Test
    public void testTpcdsQ75()
            throws Exception
    {
        assertQuery(getTpcdsQuery("75"));
    }

    @Test
    public void testTpcdsQ76()
            throws Exception
    {
        assertQuery(getTpcdsQuery("76"));
    }

    @Test
    public void testTpcdsQ77()
            throws Exception
    {
        assertQuery(getTpcdsQuery("77"));
    }

    @Test
    public void testTpcdsQ78()
            throws Exception
    {
        assertQuery(getTpcdsQuery("78"));
    }

    @Test
    public void testTpcdsQ79()
            throws Exception
    {
        assertQuery(getTpcdsQuery("79"));
    }

    @Test
    public void testTpcdsQ80()
            throws Exception
    {
        assertQuery(getTpcdsQuery("80"));
    }

    @Test
    public void testTpcdsQ81()
            throws Exception
    {
        assertQuery(getTpcdsQuery("81"));
    }

    @Test
    public void testTpcdsQ82()
            throws Exception
    {
        assertQuery(getTpcdsQuery("82"));
    }

    @Test
    public void testTpcdsQ83()
            throws Exception
    {
        assertQuery(getTpcdsQuery("83"));
    }

    @Test
    public void testTpcdsQ84()
            throws Exception
    {
        assertQuery(getTpcdsQuery("84"));
    }

    @Test
    public void testTpcdsQ85()
            throws Exception
    {
        assertQuery(getTpcdsQuery("85"));
    }

    @Test
    public void testTpcdsQ86()
            throws Exception
    {
        assertQuery(getTpcdsQuery("86"));
    }

    @Test
    public void testTpcdsQ87()
            throws Exception
    {
        assertQuery(getTpcdsQuery("87"));
    }

    @Test
    public void testTpcdsQ88()
            throws Exception
    {
        assertQuery(getTpcdsQuery("88"));
    }

    @Test
    public void testTpcdsQ89()
            throws Exception
    {
        assertQueryFails(getTpcdsQuery("89"), "[\\s\\S]*Cannot nest aggregations inside aggregation[\\s\\S]*");
    }

    @Test
    public void testTpcdsQ90()
            throws Exception
    {
        assertQuery(getTpcdsQuery("90"));
    }

    @Test
    public void testTpcdsQ91()
            throws Exception
    {
        assertQuery(getTpcdsQuery("91"));
    }

    @Test
    public void testTpcdsQ92()
            throws Exception
    {
        assertQuery(getTpcdsQuery("92"));
    }

    @Test
    public void testTpcdsQ93()
            throws Exception
    {
        assertQuery(getTpcdsQuery("93"));
    }

    @Test
    public void testTpcdsQ94()
            throws Exception
    {
        assertQuery(getTpcdsQuery("94"));
    }

    @Test
    public void testTpcdsQ95()
            throws Exception
    {
        assertQuery(getTpcdsQuery("95"));
    }

    @Test
    public void testTpcdsQ96()
            throws Exception
    {
        assertQuery(getTpcdsQuery("96"));
    }

    @Test
    public void testTpcdsQ97()
            throws Exception
    {
        assertQuery(getTpcdsQuery("97"));
    }

    @Test(enabled = false)
    public void testTpcdsQ98()
            throws Exception
    {
        assertQuery(getTpcdsQuery("98"));
    }

    @Test
    public void testTpcdsQ99()
            throws Exception
    {
        assertQuery(getTpcdsQuery("99"));
    }
}