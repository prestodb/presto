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
package io.prestosql.operator.aggregation;

// Lambda has to be compiled into a dedicated class, as functions might be stateful (e.g. use CachedInstanceBinder)
public interface LambdaProvider
{
    // To support capture, we can enrich the interface into
    // getLambda(Object[] capturedValues)

    // The lambda capture is done through invokedynamic, and the CallSite will be cached after
    // the first call. Thus separate classes have to be generated for different captures.
    Object getLambda();
}
