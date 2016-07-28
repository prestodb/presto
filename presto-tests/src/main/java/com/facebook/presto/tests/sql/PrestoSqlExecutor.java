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
package com.facebook.presto.tests.sql;

import com.facebook.presto.testing.QueryRunner;

public class PrestoSqlExecutor
        implements SqlExecutor
{
    private final QueryRunner queryRunner;

    public PrestoSqlExecutor(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
    }

    @Override
    public void execute(String sql)
    {
        try {
            queryRunner.execute(queryRunner.getDefaultSession(), sql);
        }
        catch (Throwable e) {
            throw new RuntimeException("Error executing sql:\n" + sql, e);
        }
    }
}
