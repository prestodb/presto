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

import com.facebook.presto.hive.AbstractTestCteExecution;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

public abstract class AbstractTestNativeCteExecution
        extends AbstractTestCteExecution
{
    protected QueryRunner createQueryRunner(String storageFormat, boolean singleNode)
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setExtraProperties(ImmutableMap.of(
                        "query.cte-partitioning-provider-catalog", "hive",
                        "single-node-execution-enabled", "" + singleNode))
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.enable-parquet-dereference-pushdown", "true")
                        .put("hive.temporary-table-compression-codec", "NONE")
                        .put("hive.temporary-table-storage-format", storageFormat)
                        .build())
                .build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();

        // This call avoids casting date fields to VARCHAR. DWRF requires the conversion, but this test uses PARQUET
        // which doesn't have this restriction. The change is needed because several CTE tests use
        // EXTRACT functions from date columns.
        NativeQueryRunnerUtils.createAllTables(queryRunner, false);
    }

    @Override
    @Test(enabled = false)
    // Char type is not supported in Prestissimo.
    public void testPersistentCteWithChar() {}

    @Override
    // Unsupported nested encoding in Velox Parquet Writer.
    // Error : VeloxRuntimeError: vec.valueVector() == nullptr || vec.wrappedVector()->isFlatEncoding()
    // An unsupported nested encoding was found. Operator: TableWrite(1)
    @Test(enabled = false)
    public void testPersistentCteWithStructTypes() {}

    @Override
    // Unsupported nested encoding in Velox Parquet Writer.
    // Error : VeloxRuntimeError: vec.valueVector() == nullptr || vec.wrappedVector()->isFlatEncoding()
    // An unsupported nested encoding was found. Operator: TableWrite(1)
    @Test(enabled = false)
    public void testPersistentCteWithMap() {}

    @Override
    // Unsupported nested encoding in Velox Parquet Writer.
    // Error : VeloxRuntimeError: vec.valueVector() == nullptr || vec.wrappedVector()->isFlatEncoding()
    // An unsupported nested encoding was found. Operator: TableWrite(1)
    @Test(enabled = false)
    public void testPersistentCteWithArrayWhereInnerTypeIsNotSupported() {}

    @Override
    // Unsupported nested encoding in Velox Parquet Writer.
    // Error : VeloxRuntimeError: vec.valueVector() == nullptr || vec.wrappedVector()->isFlatEncoding()
    // An unsupported nested encoding was found. Operator: TableWrite(1)
    @Test(enabled = false)
    public void testPersistentCteWithArrayWhereInnerTypeSupported() {}

    // Native engine does not support PAGEFILE which is needed for serializing Hive non-native types.
    @Override
    @Test(enabled = false)
    public void testPersistentCteWithTimeStampWithTimeZoneType() {}
}
