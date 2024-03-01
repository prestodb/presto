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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.verifier.framework.Column;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.VerifierUtil.delimitedIdentifier;

public class SimpleColumnValidator
        implements ColumnValidator
{
    @Override
    public List<SingleColumn> generateChecksumColumns(Column column)
    {
        return ImmutableList.of(
                new SingleColumn(
                        new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(column.getExpression())),
                        Optional.of(delimitedIdentifier(getChecksumColumnAlias(column)))));
    }

    @Override
    public List<ColumnMatchResult<SimpleColumnChecksum>> validate(Column column, ChecksumResult controlResult, ChecksumResult testResult)
    {
        String checksumColumnAlias = getChecksumColumnAlias(column);
        SimpleColumnChecksum controlChecksum = new SimpleColumnChecksum(controlResult.getChecksum(checksumColumnAlias));
        SimpleColumnChecksum testChecksum = new SimpleColumnChecksum(testResult.getChecksum(checksumColumnAlias));
        return ImmutableList.of(new ColumnMatchResult<>(Objects.equals(controlChecksum, testChecksum), column, controlChecksum, testChecksum));
    }

    private static String getChecksumColumnAlias(Column column)
    {
        return column.getName() + "$checksum";
    }
}
