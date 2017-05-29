'use strict';

// See https://datatables.net/extras/colvis/
angular.module('datatables.colvis', ['datatables'])
    .config(dtColVisConfig);

/* @ngInject */
function dtColVisConfig($provide, DT_DEFAULT_OPTIONS) {
    $provide.decorator('DTOptionsBuilder', dtOptionsBuilderDecorator);

    function dtOptionsBuilderDecorator($delegate) {
        var newOptions = $delegate.newOptions;
        var fromSource = $delegate.fromSource;
        var fromFnPromise = $delegate.fromFnPromise;

        $delegate.newOptions = function() {
            return _decorateOptions(newOptions);
        };
        $delegate.fromSource = function(ajax) {
            return _decorateOptions(fromSource, ajax);
        };
        $delegate.fromFnPromise = function(fnPromise) {
            return _decorateOptions(fromFnPromise, fnPromise);
        };

        return $delegate;

        function _decorateOptions(fn, params) {
            var options = fn(params);
            options.withColVis = withColVis;
            options.withColVisOption = withColVisOption;
            options.withColVisStateChange = withColVisStateChange;
            return options;

            /**
             * Add colVis compatibility
             * @returns {DTOptions} the options
             */
            function withColVis() {
                var colVisPrefix = 'C';
                options.dom = options.dom ? options.dom : DT_DEFAULT_OPTIONS.dom;
                if (options.dom.indexOf(colVisPrefix) === -1) {
                    options.dom = colVisPrefix + options.dom;
                }
                options.hasColVis = true;
                return options;
            }

            /**
             * Add option to "oColVis" option
             * @param key the key of the option to add
             * @param value an object or a function of the function
             * @returns {DTOptions} the options
             */
            function withColVisOption(key, value) {
                if (angular.isString(key)) {
                    options.oColVis = options.oColVis && options.oColVis !== null ? options.oColVis : {};
                    options.oColVis[key] = value;
                }
                return options;
            }

            /**
             * Set the state change function
             * @param fnStateChange  the state change function
             * @returns {DTOptions} the options
             */
            function withColVisStateChange(fnStateChange) {
                if (angular.isFunction(fnStateChange)) {
                    options.withColVisOption('fnStateChange', fnStateChange);
                } else {
                    throw new Error('The state change must be a function');
                }
                return options;
            }
        }
    }
}
