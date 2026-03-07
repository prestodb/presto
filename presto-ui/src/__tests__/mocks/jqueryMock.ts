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

/**
 * jQuery Setup for Tests
 *
 * Uses the SAME jQuery version as production (3.7.1 from npm package)
 * with selective mocking of only the problematic parts:
 * - jQuery plugins (sparkline, modal, tooltip) that require browser APIs
 * - Animation methods (made instant for test speed)
 *
 * Benefits:
 * - Uses exact same jQuery version as production (3.7.1)
 * - Real jQuery behavior (DOM manipulation, selectors, utilities)
 * - Utility functions work correctly ($.extend, $.isArray, $.each, etc.)
 * - Better test confidence
 * - Less maintenance burden
 * - Proper TypeScript types
 */

// Import jQuery 3.7.1 from npm - same version as production
import $ from "jquery";

/**
 * Setup jQuery with selective mocking
 * Call this once in setupTests.ts to configure jQuery for the test environment
 */
/**
 * Helper to create instant animation mock that calls callback immediately
 * @param displayAction - The display action to perform (show/hide)
 */
const createInstantAnimationMock = (displayAction: "show" | "hide") => {
    return function (this: JQuery, ...args: any[]): JQuery {
        // Perform the display action immediately
        this[displayAction]();

        // Extract and call the callback if provided
        const callback = args[args.length - 1];
        if (typeof callback === "function") {
            callback.call(this);
        }

        return this;
    };
};

export const setupJQuery = (): JQueryStatic => {
    // Mock jQuery plugins that require browser APIs or external dependencies
    // These are used in the application but don't need real implementations in tests

    // Sparkline plugin - used for rendering charts
    // @ts-expect-error - Adding plugin method to jQuery
    $.fn.sparkline = jest.fn().mockReturnThis();

    // Bootstrap modal plugin - used for showing/hiding modals
    // @ts-expect-error - Adding plugin method to jQuery
    $.fn.modal = jest.fn().mockReturnThis();

    // Bootstrap tooltip plugin - used for tooltips
    // @ts-expect-error - Adding plugin method to jQuery
    $.fn.tooltip = jest.fn().mockReturnThis();

    // Mock animation methods to be instant in tests (for speed)
    // Override animate to skip animation and call complete callback immediately
    $.fn.animate = function (this: JQuery, ...args: any[]): JQuery {
        const options = args[1];
        if (typeof options === "object" && options?.complete) {
            options.complete.call(this);
        }
        return this;
    };

    // Override fade/slide methods to be instant using helper
    $.fn.fadeIn = createInstantAnimationMock("show");
    $.fn.fadeOut = createInstantAnimationMock("hide");
    $.fn.slideDown = createInstantAnimationMock("show");
    $.fn.slideUp = createInstantAnimationMock("hide");

    // Note: We do NOT mock AJAX methods ($.ajax, $.get, $.post, etc.)
    // because Presto UI uses the fetch() API instead of jQuery AJAX.
    // The fetch() API is already mocked in apiMocks.ts.

    // Set jQuery as global for components that use it
    (global as any).$ = $;
    (global as any).jQuery = $;

    return $;
};

/**
 * Mock jQuery.get() for tests
 * This is a test helper that mocks jQuery AJAX GET requests
 * Maps URLs to response data for testing
 *
 * Supports both callback and promise-style usage:
 * - $.get(url, callback) - callback style
 * - $.get(url).done(callback) - promise style
 *
 * Uses jQuery's native Deferred object for proper promise behavior
 *
 * @param urlToDataMap - Map of URL to response data
 *
 * @example
 * mockJQueryGet({
 *   '/v1/query': [query1, query2],
 *   '/v1/info': { version: '1.0.0' }
 * });
 */
export const mockJQueryGet = (urlToDataMap: Record<string, any>): void => {
    // Mock $.get using jQuery's native Deferred for proper promise behavior
    ($ as any).get = jest.fn((url: string, callback?: (data: any) => void) => {
        const data = urlToDataMap[url];

        const deferred = $.Deferred();
        if (data !== undefined) {
            deferred.resolve(data);

            // callback style
            if (typeof callback === "function") {
                callback(data);
            }
        } else {
            deferred.reject();
        }

        return deferred.promise();
    });
};

// Made with Bob
