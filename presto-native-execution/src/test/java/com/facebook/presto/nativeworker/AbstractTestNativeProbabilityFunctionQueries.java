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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createSupplier;

public abstract class AbstractTestNativeProbabilityFunctionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createNation(queryRunner);
        createSupplier(queryRunner);
    }

    @Test
    public void testBinomialCDF()
    {
        assertQuery("SELECT binomial_cdf(CAST (nationkey AS INTEGER), 0.1, 0 ) FROM nation WHERE nationkey > 0");
        assertQuery("SELECT binomial_cdf(CAST (nationkey AS INTEGER), 0.1, CAST (regionkey AS INTEGER)) FROM nation WHERE nationkey > 0;");
    }

    @Test
    public void testCauchyCDF()
    {
        assertQuery("SELECT cauchy_cdf(nationkey, 1, 0) FROM nation");
        assertQuery("SELECT cauchy_cdf(nationkey, 1, regionkey) FROM nation");
        assertQuery("SELECT cauchy_cdf(nationkey, regionkey, 1) FROM nation WHERE regionkey > 0");
    }

    @Test
    public void testBetaCDF()
    {
        assertQuery("SELECT beta_cdf(nationkey, 0.2, 0.2) from nation where nationkey > 0");
    }

    @Test
    public void testFCDF()
    {
        assertQuery("SELECT f_cdf(nationKey, 1, 0) FROM nation WHERE nationKey > 0");
        assertQuery("SELECT f_cdf(nationKey, regionKey, 1) FROM nation WHERE nationKey > 0 AND regionkey > 0");
        assertQuery("SELECT f_cdf(nationKey, 1, regionKey) FROM nation WHERE nationKey > 0 AND regionkey > 0");
    }

    @Test
    public void testChiSquaredCDF()
    {
        assertQuery("SELECT chi_squared_cdf(acctbal, 2.0) FROM supplier WHERE acctbal > 0.0 AND acctbal < 500.0");
        assertQuery("SELECT chi_squared_cdf(acctbal, 40.0) FROM supplier WHERE acctbal > 0.0 AND acctbal < 500.0");
    }

    @Test
    public void testGammaCDF()
    {
        assertQuery("SELECT gamma_cdf(acctbal, 3.5, 0.5) FROM supplier WHERE acctbal > 0.0 AND acctbal < 999.0");
        assertQuery("SELECT gamma_cdf(1.0, acctbal, 7.0) FROM supplier WHERE acctbal > 0.0 AND acctbal < 999.0");
        assertQuery("SELECT gamma_cdf(10.0, 2.0, acctbal) FROM supplier WHERE acctbal > 0.0 AND acctbal < 999.0");
    }
}
