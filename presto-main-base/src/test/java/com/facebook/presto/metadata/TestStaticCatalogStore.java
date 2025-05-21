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
package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TestStaticCatalogStore
{
    public static final String SAMPLE_ENV_KEY = "sampleEnvKey";
    public static final String SAMPLE_ENV_VALUE = "sampleEnvValue";
    private static final Map<String, String> MOCKED_ENV = ImmutableMap.of(SAMPLE_ENV_KEY, SAMPLE_ENV_VALUE);

    @DataProvider
    public Object[][] invalidEnvVariables()
    {
        return new Object[][] {
                new Object[] {"${" + SAMPLE_ENV_KEY + "}_xyz${abc}"},
                new Object[] {"${VeryUnlikelyEnvKey}"},
        };
    }

    @DataProvider
    public Object[][] propertyValues()
    {
        return new Object[][] {
                new Object[] {"${" + SAMPLE_ENV_KEY + "}", SAMPLE_ENV_VALUE}, // only valid substitution
                new Object[] {"", ""},
                new Object[] {"abc", "abc"},
                new Object[] {"{abc", "{abc"},
                new Object[] {"{abc}", "{abc}"},
                new Object[] {"${" + SAMPLE_ENV_KEY, "${" + SAMPLE_ENV_KEY}, // missing right bracket
        };
    }

    @Test(dataProvider = "propertyValues")
    public void testEnvSubstitution(String propertyValue, String expectedSubstitutedValue)
    {
        assertEquals(StaticCatalogStore.substitutePlaceHolder(propertyValue, MOCKED_ENV), expectedSubstitutedValue);
    }

    @Test(dataProvider = "invalidEnvVariables", expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Unable to find env variable .*")
    public void testEnvSubstitutionThrowsOnMissingEnvVariable(String propertyValue)
    {
        StaticCatalogStore.substitutePlaceHolder(propertyValue, MOCKED_ENV);
    }
}
