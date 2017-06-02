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

import com.google.common.collect.ImmutableList;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RegexMethodSelector
        implements IMethodInterceptor
{
    private final List<String> patterns;

    protected RegexMethodSelector(String pattern, String... patterns)
    {
        this(ImmutableList.<String>builder()
                .add(pattern)
                .add(patterns)
                .build());
    }

    protected RegexMethodSelector(List<String> patterns)
    {
        this.patterns = requireNonNull(patterns, "patterns is null");
    }

    @Override
    public final List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context)
    {
        if (methods == null || methods.isEmpty()) {
            return methods;
        }

        return methods.stream()
                .filter(this::matches)
                .collect(toList());
    }

    private boolean matches(IMethodInstance methodInstance)
    {
        requireNonNull(methodInstance, "methodInstance is null");
        ITestNGMethod method = requireNonNull(methodInstance.getMethod(), "method is null");
        for (String pattern : patterns) {
            if (method.getMethodName().matches(pattern)) {
                return true;
            }
        }
        return false;
    }
}
