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
package com.facebook.presto.benchmark.framework;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.benchmark.prestoaction.BenchmarkPrestoActionFactory;
import com.facebook.presto.benchmark.prestoaction.PrestoActionFactory;
import com.facebook.presto.benchmark.prestoaction.PrestoClusterConfig;
import com.facebook.presto.benchmark.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.benchmark.retry.RetryConfig;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.inject.Binder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.Scopes.SINGLETON;
import static java.util.Objects.requireNonNull;

public class BenchmarkModule
        extends AbstractConfigurationAwareModule
{
    private final SqlParserOptions sqlParserOptions;
    private final ParsingOptions parsingOptions;
    private final SqlExceptionClassifier exceptionClassifier;

    public BenchmarkModule(SqlParserOptions sqlParserOptions, ParsingOptions parsingOptions, SqlExceptionClassifier exceptionClassifier)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
        this.exceptionClassifier = requireNonNull(exceptionClassifier, "exceptionClassifier is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(BenchmarkRunnerConfig.class);
        configBinder(binder).bindConfig(PrestoClusterConfig.class);
        configBinder(binder).bindConfig(RetryConfig.class);

        binder.bind(SqlExceptionClassifier.class).toInstance(exceptionClassifier);
        binder.bind(SqlParserOptions.class).toInstance(sqlParserOptions);
        binder.bind(ParsingOptions.class).toInstance(parsingOptions);
        binder.bind(SqlParser.class).in(SINGLETON);

        binder.bind(PrestoActionFactory.class).to(BenchmarkPrestoActionFactory.class).in(SINGLETON);
        binder.bind(ConcurrentPhaseExecutor.class).in(SINGLETON);

        binder.bind(BenchmarkRunner.class).in(SINGLETON);
    }
}
