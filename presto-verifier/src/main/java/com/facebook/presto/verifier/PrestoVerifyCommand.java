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
package com.facebook.presto.verifier;

import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.verifier.framework.AbstractVerifyCommand;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.airlift.airline.Command;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

@Command(name = "verify", description = "verify")
public class PrestoVerifyCommand
        extends AbstractVerifyCommand
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
    public Set<String> getCustomSourceQuerySupplierTypes()
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> getCustomEventClientTypes()
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> getCustomQueryActionTypes()
    {
        return ImmutableSet.of();
    }

    @Override
    public List<Class<? extends Predicate<SourceQuery>>> getCustomQueryFilterClasses()
    {
        return ImmutableList.of();
    }

    @Override
    public SqlExceptionClassifier getSqlExceptionClassifier()
    {
        return PrestoExceptionClassifier.defaultBuilder().build();
    }
}
