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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;

/**
 * ORC variant of the V3 deletion-vector native suite.
 *
 * <p>Currently blocked by a native DWRF read-after-write issue: Prestissimo
 * writes ORC-request tables as DWRF (its ORC-flavor format), and the DWRF
 * reader fails on the small files these tests produce with
 * "lz4 failed to decompress" / "ParseFromZeroCopyStream" on the file footer.
 * The same failure reproduces on any native CREATE + INSERT + SELECT round
 * trip with storage format ORC and is unrelated to V3 or deletion vectors --
 * {@code TestCreateTable} and the surrounding native-iceberg suite stay on
 * {@code ICEBERG_DEFAULT_STORAGE_FORMAT = "PARQUET"} for the same reason.
 *
 * <p>This class is left in place so that, once the native DWRF round-trip
 * is fixed (or the Iceberg native writer emits real ORC), V3 deletion-vector
 * coverage on ORC comes online with no test-side change. The companion
 * Parquet runner validates the V3 DV path today.
 */
public class TestPrestoNativeIcebergDeletionVectorQueriesOrcUsingThrift
        extends AbstractTestNativeIcebergDeletionVectorQueries
{
    private final String storageFormat = "ORC";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setUseThrift(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .build();
    }
}
