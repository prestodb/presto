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
package com.facebook.presto.connector.meta;

import com.google.common.base.Joiner;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;

/*
 * Determines whether a test method should run based on the @SupportedFeatures
 * and @RequiredFeatures of the test method and the connector the test is being
 * run against.
 *
 * Consider the following class hierarchy:
 * BaseTest @RequiredFeatures({FEATURE1})
 * |   test0
 * `-- SubTest @RequiredFeatures({FEATURE2})
 *     |   test1
 *     |   test2 @RequiredFeatures({FEATURE3})
 *     |-- ConnectorTest1 @SupportedFeatures({FEATURE1, FEATURE2, FEATURE3})
 *     `-- ConnectorTest2 @SupportedFeatures({FEATURE1, FEATURE2})
 *
 * SupportedTestCondition only looks for a @SupportedFeatures annotation that
 * is directly present on the test class.
 *
 * ConnectorTest1.testN has supported features {FEATURE1, FEATURE2, FEATURE3}
 * ConnectorTest2.testN has supported features {FEATURE1, FEATURE2}
 *
 * Required features are gathered from the test method, its declaring class,
 * and all of the super classes of its declaring class, recursively .
 *
 * ConnectorTestN.test0 has required features {FEATURE1}
 * ConnectorTestN.test1 has required features {FEATURE1, FEATURE2}
 * ConnectorTestN.test2 has required features {FEATURE1, FEATURE2, FEATURE3}
 *
 * A test is disabled if the test method has required features not contained in
 * its supported features.
 *
 * Gathering required features for tests in inner classes is not supported.
 * Feel free to add support if needed.
 *
 * Test interfaces can extend multiple other test interfaces. Required features
 * are correctly gathered from all extended interfaces.
 *
 * BaseTest1 @RequiredFeatures({FEATURE1})     BaseTest2 @RequiredFeatures({FEATURE2})
 *                                         \ /
 *                                       SubTest
 *                                           testMethod()
 *
 * The required features of SubTest.testMethod are {FEATURE1, FEATURE2}
 */
public class SupportedTestCondition
        implements ExecutionCondition
{
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext)
    {
        Optional<Method> testMethod = extensionContext.getTestMethod();

        if (!testMethod.isPresent()) {
            return enabled("");
        }

        Class<?> testClass = extensionContext.getRequiredTestClass();

        Optional<RequiredFeatures> methodRequires = testMethod.map(method -> method.getAnnotation(RequiredFeatures.class));
        List<RequiredFeatures> classRequires = getClassRequiredFeatures(testMethod.get().getDeclaringClass());
        Optional<SupportedFeatures> classSupports = Optional.ofNullable(testClass.getAnnotation(SupportedFeatures.class));

        Set<ConnectorFeature> requiredFeatures = Stream.concat(
                streamOfRequired(methodRequires),
                classRequires.stream().flatMap(r -> Arrays.stream(r.value())))
                .distinct()
                .collect(Collectors.toCollection(HashSet::new));

        Set<ConnectorFeature> supportedFeatures = streamOfSupported(classSupports)
                .distinct()
                .collect(toImmutableSet());

        requiredFeatures.removeAll(supportedFeatures);

        return requiredFeatures.isEmpty() ?
                enabled("All required features present") :
                disabled("Missing required features: " + Joiner.on(", ").join(requiredFeatures));
    }

    private static List<RequiredFeatures> getClassRequiredFeatures(Class<?> testClass)
    {
        assertNull(testClass.getEnclosingClass(), "Add support (and tests!) if you need it");
        Class<?>[] interfaces = testClass.getInterfaces();
        Optional<RequiredFeatures> requiredFeatures = Optional.ofNullable(testClass.getAnnotation(RequiredFeatures.class));

        return Stream.concat(
                requiredFeatures.map(Stream::of).orElse(Stream.empty()),
                Arrays.stream(interfaces)
                        .map(SupportedTestCondition::getClassRequiredFeatures)
                        .flatMap(List::stream))
                .collect(toImmutableList());
    }

    private static Stream<ConnectorFeature> streamOfRequired(Optional<RequiredFeatures> requiredFeatures)
    {
        return requiredFeatures.map(required -> Arrays.stream(required.value()))
                .orElse(Stream.empty());
    }

    private static Stream<ConnectorFeature> streamOfSupported(Optional<SupportedFeatures> supportedFeatures)
    {
        return supportedFeatures.map(supported -> Arrays.stream(supported.value()))
                .orElse(Stream.empty());
    }
}
