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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.Session;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;

import java.util.function.Function;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class TpchQueryRunnerBuilder
        extends DistributedQueryRunner.Builder
{
    private static final Session DEFAULT_SESSION = testSessionBuilder()
            .setSource("test")
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private TpchQueryRunnerBuilder()
    {
        super(DEFAULT_SESSION);
    }

    @Override
    public TpchQueryRunnerBuilder amendSession(Function<Session.SessionBuilder, Session.SessionBuilder> amendSession)
    {
        return (TpchQueryRunnerBuilder) super.amendSession(amendSession);
    }

    public static TpchQueryRunnerBuilder builder()
    {
        return new TpchQueryRunnerBuilder();
    }

    @Override
    public DistributedQueryRunner build()
            throws Exception
    {
        DistributedQueryRunner queryRunner = buildWithoutCatalogs();
        try {
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public DistributedQueryRunner buildWithoutCatalogs()
            throws Exception
    {
        DistributedQueryRunner queryRunner = super.build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
