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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.verifier.checksum.ColumnMatchResult;
import com.facebook.presto.verifier.checksum.SimpleColumnChecksum;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.VerifierUtil.PARSING_OPTIONS;
import static com.facebook.presto.verifier.resolver.FailureResolverTestUtil.binary;
import static com.facebook.presto.verifier.resolver.FailureResolverTestUtil.createMismatchedColumn;

public class TestIgnoredFunctionsMismatchResolver
        extends AbstractTestResultMismatchResolver
{
    private static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON));

    public TestIgnoredFunctionsMismatchResolver()
    {
        super(new IgnoredFunctionsMismatchResolver(new IgnoredFunctionsMismatchResolverConfig().setFunctions("rand,presto.default.arbitrary")));
    }

    @Test
    public void testDefault()
    {
        ColumnMatchResult<?> mismatchedColumn = createMismatchedColumn(VARCHAR, new SimpleColumnChecksum(binary(0xa)), new SimpleColumnChecksum(binary(0xb)));

        // resolved
        assertResolved(createBundle("CREATE TABLE test AS SELECT rand() x FROM source"), mismatchedColumn);
        assertResolved(createBundle("CREATE TABLE test AS SELECT presto.default.rand() x FROM source"), mismatchedColumn);
        assertResolved(createBundle("SELECT arbitrary(x) FROM source"), mismatchedColumn);
        assertResolved(createBundle("INSERT INTO target SELECT presto.default.arbitrary(x) FROM source"), mismatchedColumn);

        assertResolved(createBundle("SELECT arbitrary(rand()) FROM source"), mismatchedColumn);
        assertResolved(createBundle("SELECT sum(rand()) FROM source"), mismatchedColumn);

        // not resolved
        assertNotResolved(createBundle("SELECT count() FROM source"), mismatchedColumn);
    }

    private static QueryObjectBundle createBundle(String query)
    {
        return new QueryObjectBundle(
                QualifiedName.of("test"),
                ImmutableList.of(),
                sqlParser.createStatement(query, PARSING_OPTIONS),
                ImmutableList.of(),
                CONTROL);
    }
}
