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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/*
 * On an abstract test class, informs the annotation processor that it needs to
 * generate concrete subclasses for @TestableConnectors that support all of the
 * requiredFeatures.
 *
 * Can additionally be specified on a test method within an annotated test
 * class to specify test methods that logically belong inside the test class,
 * but have requirements beyond what the majority of the methods in the test
 * class have. In this case, the concrete subclass will @Override the annotated
 * test method with an empty test.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ConnectorTest
{
    /*
     * A testName is required when annotating test classes, but is ignored when
     * individual test methods are annotated.
     *
     * The annotation processor is responsible for enforcing this requirement.
     */
    String testName() default "";

    ConnectorFeature[] requiredFeatures();
}
