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
package com.facebook.presto;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

public class Retry
        implements IRetryAnalyzer
{
    private static final int maxTry = 5;
    private int count;

    @Override
    public boolean retry(ITestResult iTestResult)
    {
        if (!iTestResult.isSuccess()) {
            if (count < maxTry) {
                count++;
                System.out.println("Retry #" + count + " for test: " + iTestResult.getMethod().getMethodName());
                iTestResult.setStatus(ITestResult.FAILURE);
                return true;
            }
            else {
                iTestResult.setStatus(ITestResult.FAILURE);
            }
        }
        else {
            iTestResult.setStatus(ITestResult.SUCCESS);
        }
        return false;
    }
}
