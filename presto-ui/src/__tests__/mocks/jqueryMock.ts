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
 * Comprehensive jQuery mock for testing
 * Provides mock implementations of jQuery methods used throughout the application
 */

// Create a jQuery promise-like object
export const createJQueryPromise = (data: any) => {
    const promise = {
        then: jest.fn((callback) => {
            if (callback) callback(data);
            return promise;
        }),
        done: jest.fn((callback) => {
            if (callback) callback(data);
            return promise;
        }),
        fail: jest.fn(() => promise),
        always: jest.fn((callback) => {
            if (callback) callback(data);
            return promise;
        }),
        catch: jest.fn(() => promise),
    };
    return promise;
};

// Create the main jQuery mock function
export const createJQueryMock = () => {
    const jQueryMock = jest.fn((_selector) => {
        const element = {
            // DOM manipulation
            html: jest.fn().mockReturnThis(),
            text: jest.fn().mockReturnThis(),
            val: jest.fn().mockReturnThis(),
            attr: jest.fn().mockReturnThis(),
            css: jest.fn().mockReturnThis(),
            addClass: jest.fn().mockReturnThis(),
            removeClass: jest.fn().mockReturnThis(),
            toggleClass: jest.fn().mockReturnThis(),

            // Events
            on: jest.fn().mockReturnThis(),
            off: jest.fn().mockReturnThis(),
            trigger: jest.fn().mockReturnThis(),
            click: jest.fn().mockReturnThis(),
            change: jest.fn().mockReturnThis(),
            bind: jest.fn().mockReturnThis(),
            unbind: jest.fn().mockReturnThis(),

            // Traversal
            find: jest.fn().mockReturnThis(),
            parent: jest.fn().mockReturnThis(),
            parents: jest.fn().mockReturnThis(),
            children: jest.fn().mockReturnThis(),
            each: jest.fn(function (callback) {
                // Call callback with mock context
                callback.call(this, 0, {});
                return this;
            }),
            filter: jest.fn().mockReturnThis(),
            eq: jest.fn().mockReturnThis(),
            first: jest.fn().mockReturnThis(),
            last: jest.fn().mockReturnThis(),

            // Dimensions
            width: jest.fn().mockReturnValue(100),
            height: jest.fn().mockReturnValue(100),
            innerWidth: jest.fn().mockReturnValue(100),
            innerHeight: jest.fn().mockReturnValue(100),
            outerWidth: jest.fn().mockReturnValue(100),
            outerHeight: jest.fn().mockReturnValue(100),

            // Position
            offset: jest.fn().mockReturnValue({ top: 0, left: 0 }),
            position: jest.fn().mockReturnValue({ top: 0, left: 0 }),
            scrollTop: jest.fn().mockReturnValue(0),
            scrollLeft: jest.fn().mockReturnValue(0),

            // Data
            data: jest.fn().mockReturnThis(),

            // Visibility
            show: jest.fn().mockReturnThis(),
            hide: jest.fn().mockReturnThis(),
            is: jest.fn().mockReturnValue(false),
            toggle: jest.fn().mockReturnThis(),

            // Bootstrap plugins
            modal: jest.fn().mockReturnThis(),
            tooltip: jest.fn().mockReturnThis(),

            // Sparkline plugin
            sparkline: jest.fn().mockReturnThis(),

            // Utilities
            remove: jest.fn().mockReturnThis(),
            append: jest.fn().mockReturnThis(),
            prepend: jest.fn().mockReturnThis(),
            empty: jest.fn().mockReturnThis(),
            replaceWith: jest.fn().mockReturnThis(),

            // Properties
            length: 1,
            0: {},
        };

        return element;
    });

    // jQuery static methods
    // Cast to any to allow dynamic property assignment on Jest mock
    const mock = jQueryMock as any;

    mock.ajax = jest.fn().mockResolvedValue({});

    mock.get = jest.fn((_url, _callback) => {
        const data: any[] = [];
        const promise = createJQueryPromise(data);
        if (_callback) {
            setTimeout(() => _callback(data), 0);
        }
        return promise;
    });

    mock.post = jest.fn((_url, _data, _callback) => {
        const responseData = {};
        const promise = createJQueryPromise(responseData);
        if (_callback) {
            setTimeout(() => _callback(responseData), 0);
        }
        return promise;
    });

    mock.extend = jest.fn((target, ...sources) => Object.assign(target || {}, ...sources));

    mock.Event = jest.fn((type) => ({
        type,
        isPropagationStopped: jest.fn().mockReturnValue(false),
        preventDefault: jest.fn(),
        stopPropagation: jest.fn(),
    }));

    mock.fn = {};

    mock.isPlainObject = jest.fn((obj) => Object.prototype.toString.call(obj) === "[object Object]");

    return mock;
};

/**
 * Helper function to mock jQuery.get with specific responses
 * @param responses - Object mapping URLs to response data
 */
export const mockJQueryGet = (responses: Record<string, any>) => {
    ((global as any).$ as any).get.mockImplementation((url: string, callback?: (data: any) => void) => {
        const data = responses[url] || [];
        const promise = {
            done: jest.fn((_cb) => {
                if (_cb) setTimeout(() => _cb(data), 0);
                return promise;
            }),
            fail: jest.fn(() => promise),
            always: jest.fn((_cb) => {
                if (_cb) setTimeout(() => _cb(data), 0);
                return promise;
            }),
        };
        if (callback) {
            setTimeout(() => callback(data), 0);
        }
        return promise;
    });
};

// Made with Bob
