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
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Arrays.stream;
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

        List<ConnectorFeature> classRequires = getClassRequiredFeatures(testMethod.get().getDeclaringClass());

        Set<ConnectorFeature> requiredFeatures = Stream.concat(
                streamOfRequired(testMethod.get()),
                classRequires.stream())
                .collect(toImmutableSet());

        Set<ConnectorFeature> supportedFeatures = streamOfSupported(testClass)
                .collect(toImmutableSet());

        Set<ConnectorFeature> missingFeatures = Sets.difference(requiredFeatures, supportedFeatures);

        return missingFeatures.isEmpty() ?
                enabled("All required features present") :
                disabled("Missing required features: " + Joiner.on(", ").join(missingFeatures));
    }

    private static List<ConnectorFeature> getClassRequiredFeatures(Class<?> testClass)
    {
        Preconditions.checkState(testClass.getEnclosingClass() == null, "Inner classes not supported as test classes. Add support (and tests!) if you need it");
        Class<?>[] interfaces = testClass.getInterfaces();

        return Streams.concat(
                streamOfRequired(testClass),
                stream(interfaces)
                        .map(SupportedTestCondition::getClassRequiredFeatures)
                        .flatMap(List::stream))
                .collect(toImmutableList());
    }

    private static Stream<ConnectorFeature> streamOfRequired(@Nullable AnnotatedElement element)
    {
        if (element == null) {
            return Stream.empty();
        }

        return Optional.ofNullable(element.getAnnotation(RequiredFeatures.class))
                .map(required -> stream(required.value()))
                .orElse(Stream.empty());
    }

    private static Stream<ConnectorFeature> streamOfSupported(Class<?> testClass)
    {
        return Optional.ofNullable(testClass.getAnnotation(SupportedFeatures.class))
                .map(required -> stream(required.value()))
                .orElse(Stream.empty());
    }
}
