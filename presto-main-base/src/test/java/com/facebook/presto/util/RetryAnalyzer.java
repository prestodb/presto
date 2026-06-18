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
package com.facebook.presto.util;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

import java.lang.reflect.Method;

import static com.facebook.presto.util.RetryCount.DEFAULT_RETRY_COUNT;

public class RetryAnalyzer
        implements IRetryAnalyzer
{
    private int retryCount;
    private int maxRetryCount;

    @Override
    public boolean retry(ITestResult result)
    {
        if (maxRetryCount == 0) {
            Method method = result.getMethod().getConstructorOrMethod().getMethod();
            RetryCount annotation = method.getAnnotation(RetryCount.class);
            maxRetryCount = (annotation != null) ? annotation.value() : DEFAULT_RETRY_COUNT;
        }
        return retryCount++ < maxRetryCount;
    }
}
