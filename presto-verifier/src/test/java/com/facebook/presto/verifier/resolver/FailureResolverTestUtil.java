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
package com.facebook.presto.verifier.resolver;

import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.verifier.checksum.ColumnChecksum;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.framework.Column;
import com.facebook.presto.verifier.framework.DataMatchResult;

import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.COLUMN_MISMATCH;
import static java.util.Arrays.asList;

public class FailureResolverTestUtil
{
    private FailureResolverTestUtil() {}

    public static <T extends ColumnChecksum> ColumnMatchResult<T> createMismatchedColumn(Type type, T controlChecksum, T testChecksum)
    {
        Column column = Column.create(type.getDisplayName(), new Identifier(type.getDisplayName(), true), type);
        return new ColumnMatchResult<>(false, column, controlChecksum, testChecksum);
    }

    public static DataMatchResult createMatchResult(ColumnMatchResult<?>... mismatchedColumns)
    {
        return new DataMatchResult(COLUMN_MISMATCH, Optional.empty(), OptionalLong.of(1L), OptionalLong.of(1L), asList(mismatchedColumns));
    }

    public static SqlVarbinary binary(int data)
    {
        return new SqlVarbinary(new byte[] {(byte) data});
    }
}
