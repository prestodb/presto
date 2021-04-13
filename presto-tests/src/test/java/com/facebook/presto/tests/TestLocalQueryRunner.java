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
package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;

public class TestLocalQueryRunner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return TestLocalQueries.createLocalQueryRunner();
    }

    @Test
    public void testSimpleQuery()
    {
        assertQuery("SELECT * FROM nation");
    }

    @Test
    public void testAnalyzeAccessControl()
    {
        assertAccessAllowed("ANALYZE nation");
        assertAccessDenied("ANALYZE nation", "Cannot ANALYZE \\(missing insert privilege\\) table .*.nation.*", privilege("nation", INSERT_TABLE));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*nationkey.*] in table or view .*.nation", privilege("nationkey", SELECT_COLUMN));
    }
}
