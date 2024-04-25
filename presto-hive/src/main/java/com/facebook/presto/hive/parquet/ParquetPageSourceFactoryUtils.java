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

package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.crypto.HiddenColumnException;

import java.io.FileNotFoundException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;

public class ParquetPageSourceFactoryUtils
{
    private ParquetPageSourceFactoryUtils() {}

    public static PrestoException mapToPrestoException(Exception e, Path path, HiveFileSplit fileSplit)
    {
        if (e instanceof PrestoException) {
            throw (PrestoException) e;
        }
        if (e instanceof ParquetCorruptionException) {
            throw new PrestoException(HIVE_BAD_DATA, e);
        }
        if (e instanceof AccessControlException) {
            throw new PrestoException(PERMISSION_DENIED, e.getMessage(), e);
        }
        if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                e instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
        String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, fileSplit.getStart(), fileSplit.getLength(), e.getMessage());
        if (e.getClass().getSimpleName().equals("BlockMissingException")) {
            throw new PrestoException(HIVE_MISSING_DATA, message, e);
        }
        if (e instanceof HiddenColumnException) {
            message = format("User does not have access to encryption key for encrypted column = %s. If returning 'null' for encrypted " +
                    "columns is acceptable to your query, please add 'set session hive.read_null_masked_parquet_encrypted_value_enabled=true' before your query", ((HiddenColumnException) e).getColumn());
            throw new PrestoException(PERMISSION_DENIED, message, e);
        }
        throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
    }
}
