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
package com.facebook.presto.benchmark;

import com.facebook.presto.benchmark.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.benchmark.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.airlift.airline.Command;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;

@Command(name = "benchmark")
public class PrestoBenchmarkCommand
        extends AbstractBenchmarkCommand
{
    @Override
    public List<Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    @Override
    public SqlParserOptions getSqlParserOptions()
    {
        return new SqlParserOptions();
    }

    @Override
    public ParsingOptions getParsingOptions()
    {
        return ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build();
    }

    @Override
    public Set<String> getCustomBenchmarkSuiteSupplierTypes()
    {
        return ImmutableSet.of();
    }

    @Override
    public SqlExceptionClassifier getExceptionClassifier()
    {
        return new PrestoExceptionClassifier(ImmutableSet.of());
    }
}
