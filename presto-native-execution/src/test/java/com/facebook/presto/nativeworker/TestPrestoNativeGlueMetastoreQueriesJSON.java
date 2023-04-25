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
package com.facebook.presto.nativeworker;

/**
 * We only test JSON variant of the AbstractTestNativeGlueMetastoreQueries test suite.
 * This is to reduce the overall test duration because the test primarily verifies the functionality of the Glue
 * catalog with Prestissimo and testing Thrift variant is not necessary.
 *
 * The Thrift tests could be easily added in the future if required.
 */
public class TestPrestoNativeGlueMetastoreQueriesJSON
        extends AbstractTestNativeGlueMetastoreQueries
{
    public TestPrestoNativeGlueMetastoreQueriesJSON()
    {
        super(false);
    }
}
