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
//@flow

import React, { Suspense, lazy } from "react";

function LazyComponent<T>(
  filename: string,
): (props: any) => React.Element<any> {
  const Component = lazy<T>(
    () =>
      // $FlowIgnore[unsupported-syntax] - Flow doesn't support dynamic imports with variables
      import(`./components/${filename}`),
  );
  const LazyWrapper = (props: any) => (
    <Suspense fallback={<div className="loader">Loading...</div>}>
      <Component {...props} />
    </Suspense>
  );
  return LazyWrapper;
}

export default LazyComponent;
