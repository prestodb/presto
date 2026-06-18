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

package com.facebook.presto.ml;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import org.testng.annotations.BeforeClass;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;

abstract class AbstractTestMLFunctions
        extends AbstractTestFunctions
{
    public AbstractTestMLFunctions()
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
    }

    @BeforeClass
    protected void registerFunctions()
    {
        functionAssertions.getMetadata().registerBuiltInFunctions(
                extractFunctions(new MLPlugin().getFunctions()));
    }
}
