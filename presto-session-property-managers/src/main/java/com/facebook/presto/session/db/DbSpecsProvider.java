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
package com.facebook.presto.session.db;

import com.facebook.presto.session.SessionMatchSpec;

import java.util.List;
import java.util.function.Supplier;

/**
 * This interface was created to separate the scheduling logic for {@link SessionMatchSpec} loading. This also helps
 * us test the core logic of {@link DbSessionPropertyManager} in a modular fashion by letting us use a test
 * implementation of this interface.
 */
public interface DbSpecsProvider
        extends Supplier<List<SessionMatchSpec>>
{
}
