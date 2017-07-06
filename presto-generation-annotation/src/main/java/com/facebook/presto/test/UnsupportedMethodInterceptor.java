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
package com.facebook.presto.test;

import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;

import java.util.List;

import static com.facebook.presto.test.FeatureUtil.connectorSupportsTestMethod;
import static java.util.stream.Collectors.toList;

public class UnsupportedMethodInterceptor
        implements IMethodInterceptor
{
    @Override
    public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context)
    {
        return methods.stream()
                .filter(method -> {
                    Class<?> clazz = method.getInstance().getClass();
                    return connectorSupportsTestMethod(clazz, clazz, method.getMethod().getConstructorOrMethod().getMethod());
                })
                .collect(toList());
    }
}
