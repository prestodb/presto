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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverFactory;
import com.google.inject.Module;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public interface VerifyCommand
        extends Runnable
{
    List<Module> getAdditionalModules();

    SqlParserOptions getSqlParserOptions();

    Set<String> getCustomSourceQuerySupplierTypes();

    Set<String> getCustomEventClientTypes();

    List<Class<? extends Predicate<SourceQuery>>> getCustomQueryFilterClasses();

    SqlExceptionClassifier getSqlExceptionClassifier();

    List<FailureResolverFactory> getFailureResolverFactories();

    List<Property> getTablePropertyOverrides();
}
