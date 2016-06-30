/**************************************************************************
* AngularJS-nvD3, v1.0.0-rc; MIT License; 29/06/2015 15:12
* http://krispo.github.io/angular-nvd3
**************************************************************************/
(function(){

    'use strict';

    angular.module('nvd3', [])

        .directive('nvd3', ['nvd3Utils', function(nvd3Utils){
            return {
                restrict: 'AE',
                scope: {
                    data: '=',      //chart data, [required]
                    options: '=',   //chart options, according to nvd3 core api, [required]
                    api: '=?',      //directive global api, [optional]
                    events: '=?',   //global events that directive would subscribe to, [optional]
                    config: '=?'    //global directive configuration, [optional]
                },
                link: function(scope, element, attrs){
                    var defaultConfig = {
                        extended: false,
                        visible: true,
                        disabled: false,
                        autorefresh: true,
                        refreshDataOnly: false,
                        deepWatchData: true,
                        debounce: 10 // default 10ms, time silence to prevent refresh while multiple options changes at a time
                    };

                    //basic directive configuration
                    scope._config = angular.extend(defaultConfig, scope.config);

                    //directive global api
                    scope.api = {
                        // Fully refresh directive
                        refresh: function(){
                            scope.api.updateWithOptions(scope.options);
                        },

                        // Update chart layout (for example if container is resized)
                        update: function() {
                            scope.chart.update();
                        },

                        // Update chart with new options
                        updateWithOptions: function(options){
                            // Clearing
                            scope.api.clearElement();

                            // Exit if options are not yet bound
                            if (angular.isDefined(options) === false) return;

                            // Exit if chart is hidden
                            if (!scope._config.visible) return;

                            // Initialize chart with specific type
                            scope.chart = nv.models[options.chart.type]();

                            // Generate random chart ID
                            scope.chart.id = Math.random().toString(36).substr(2, 15);

                            angular.forEach(scope.chart, function(value, key){
                                if (key[0] === '_');
                                else if ([
                                    'clearHighlights',
                                    'highlightPoint',
                                    'id',
                                    'options',
                                    'resizeHandler',
                                    'state'
                                ].indexOf(key) >= 0);

                                else if (key === 'dispatch') {
                                    if (options.chart[key] === undefined || options.chart[key] === null) {
                                        if (scope._config.extended) options.chart[key] = {};
                                    }
                                    configureEvents(scope.chart[key], options.chart[key]);
                                }

                                else if ([
                                    'bars',
                                    'bars1',
                                    'bars2',
                                    'boxplot',
                                    'bullet',
                                    'controls',
                                    'discretebar',
                                    'distX',
                                    'distY',
                                    'interactiveLayer',
                                    'legend',
                                    'lines',
                                    'lines1',
                                    'lines2',
                                    'multibar',
                                    'pie',
                                    'scatter',
                                    'sparkline',
                                    'stack1',
                                    'stack2',
                                    'sunburst',
                                    'tooltip',
                                    'x2Axis',
                                    'xAxis',
                                    'y1Axis',
                                    'y2Axis',
                                    'y3Axis',
                                    'y4Axis',
                                    'yAxis',
                                    'yAxis1',
                                    'yAxis2'
                                ].indexOf(key) >= 0 ||
                                        // stacked is a component for stackedAreaChart, but a boolean for multiBarChart and multiBarHorizontalChart
                                        (key === 'stacked' && options.chart.type === 'stackedAreaChart')) {
                                    if (options.chart[key] === undefined || options.chart[key] === null) {
                                        if (scope._config.extended) options.chart[key] = {};
                                    }
                                    configure(scope.chart[key], options.chart[key], options.chart.type);
                                }

                                //TODO: need to fix bug in nvd3
                                else if ((key === 'xTickFormat' || key === 'yTickFormat') && options.chart.type === 'lineWithFocusChart');

                                else if (options.chart[key] === undefined || options.chart[key] === null){
                                    if (scope._config.extended) options.chart[key] = value();
                                }

                                else scope.chart[key](options.chart[key]);
                            });

                            // Update with data
                            scope.api.updateWithData(scope.data);

                            // Configure wrappers
                            if (options['title'] || scope._config.extended) configureWrapper('title');
                            if (options['subtitle'] || scope._config.extended) configureWrapper('subtitle');
                            if (options['caption'] || scope._config.extended) configureWrapper('caption');


                            // Configure styles
                            if (options['styles'] || scope._config.extended) configureStyles();

                            nv.addGraph(function() {
                                // Remove resize handler. Due to async execution should be placed here, not in the clearElement
                                if (scope.chart.resizeHandler) scope.chart.resizeHandler.clear();
                                // Update the chart when window resizes
                                scope.chart.resizeHandler = nv.utils.windowResize(function() { scope.chart.update && scope.chart.update(); });
                                return scope.chart;
                            }, options.chart['callback']);
                        },

                        // Update chart with new data
                        updateWithData: function (data){
                            if (data) {
                                // TODO this triggers one more refresh. Refactor it!
                                scope.options.chart.transitionDuration = +scope.options.chart.transitionDuration || 250;
                                // remove whole svg element with old data
                                d3.select(element[0]).select('svg').remove();

                                // Select the current element to add <svg> element and to render the chart in
                                d3.select(element[0]).append('svg')
                                    .attr('height', scope.options.chart.height)
                                    .attr('width', scope.options.chart.width  || '100%')
                                    .datum(data)
                                    .transition().duration(scope.options.chart.transitionDuration)
                                    .call(scope.chart);
                            }
                        },

                        // Fully clear directive element
                        clearElement: function (){
                            element.find('.title').remove();
                            element.find('.subtitle').remove();
                            element.find('.caption').remove();
                            element.empty();

                            // To be compatible with old nvd3 (v1.7.1)
                            if (nv.graphs && scope.chart) {
                                for(var i = nv.graphs.length - 1; i >= 0; i--) {
                                    if(nv.graphs[i].id === scope.chart.id) {
                                        nv.graphs.splice(i, 1);
                                    }
                                }
                            }
                            if (nv.tooltip && nv.tooltip.cleanup) {
                                nv.tooltip.cleanup();
                            }
                            scope.chart = null;
                        },

                        // Get full directive scope
                        getScope: function(){ return scope; }
                    };

                    // Configure the chart model with the passed options
                    function configure(chart, options, chartType){
                        if (chart && options){
                            angular.forEach(chart, function(value, key){
                                if (key[0] === '_');
                                else if (key === 'dispatch') {
                                    if (options[key] === undefined || options[key] === null) {
                                        if (scope._config.extended) options[key] = {};
                                    }
                                    configureEvents(value, options[key]);
                                }
                                else if ([
                                    'axis',
                                    'clearHighlights',
                                    'defined',
                                    'highlightPoint',
                                    'nvPointerEventsClass',
                                    'options',
                                    'rangeBand',
                                    'rangeBands',
                                    'scatter'
                                ].indexOf(key) === -1) {
                                    if (options[key] === undefined || options[key] === null){
                                        if (scope._config.extended) options[key] = value();
                                    }
                                    else chart[key](options[key]);
                                }
                            });
                        }
                    }

                    // Subscribe to the chart events (contained in 'dispatch')
                    // and pass eventHandler functions in the 'options' parameter
                    function configureEvents(dispatch, options){
                        if (dispatch && options){
                            angular.forEach(dispatch, function(value, key){
                                if (options[key] === undefined || options[key] === null){
                                    if (scope._config.extended) options[key] = value.on;
                                }
                                else dispatch.on(key + '._', options[key]);
                            });
                        }
                    }

                    // Configure 'title', 'subtitle', 'caption'.
                    // nvd3 has no sufficient models for it yet.
                    function configureWrapper(name){
                        var _ = nvd3Utils.deepExtend(defaultWrapper(name), scope.options[name] || {});

                        if (scope._config.extended) scope.options[name] = _;

                        var wrapElement = angular.element('<div></div>').html(_['html'] || '')
                            .addClass(name).addClass(_.className)
                            .removeAttr('style')
                            .css(_.css);

                        if (!_['html']) wrapElement.text(_.text);

                        if (_.enable) {
                            if (name === 'title') element.prepend(wrapElement);
                            else if (name === 'subtitle') element.find('.title').after(wrapElement);
                            else if (name === 'caption') element.append(wrapElement);
                        }
                    }

                    // Add some styles to the whole directive element
                    function configureStyles(){
                        var _ = nvd3Utils.deepExtend(defaultStyles(), scope.options['styles'] || {});

                        if (scope._config.extended) scope.options['styles'] = _;

                        angular.forEach(_.classes, function(value, key){
                            value ? element.addClass(key) : element.removeClass(key);
                        });

                        element.removeAttr('style').css(_.css);
                    }

                    // Default values for 'title', 'subtitle', 'caption'
                    function defaultWrapper(_){
                        switch (_){
                            case 'title': return {
                                enable: false,
                                text: 'Write Your Title',
                                className: 'h4',
                                css: {
                                    width: scope.options.chart.width + 'px',
                                    textAlign: 'center'
                                }
                            };
                            case 'subtitle': return {
                                enable: false,
                                text: 'Write Your Subtitle',
                                css: {
                                    width: scope.options.chart.width + 'px',
                                    textAlign: 'center'
                                }
                            };
                            case 'caption': return {
                                enable: false,
                                text: 'Figure 1. Write Your Caption text.',
                                css: {
                                    width: scope.options.chart.width + 'px',
                                    textAlign: 'center'
                                }
                            };
                        }
                    }

                    // Default values for styles
                    function defaultStyles(){
                        return {
                            classes: {
                                'with-3d-shadow': true,
                                'with-transitions': true,
                                'gallery': false
                            },
                            css: {}
                        };
                    }

                    /* Event Handling */
                    // Watching on options changing
                    scope.$watch('options', nvd3Utils.debounce(function(newOptions){
                        if (!scope._config.disabled && scope._config.autorefresh) scope.api.refresh();
                    }, scope._config.debounce, true), true);

                    // Watching on data changing
                    scope.$watch('data', function(newData, oldData){
                        if (newData !== oldData && scope.chart){
                            if (!scope._config.disabled && scope._config.autorefresh) {
                                scope._config.refreshDataOnly ? scope.chart.update() : scope.api.refresh(); // if wanted to refresh data only, use chart.update method, otherwise use full refresh.
                            }
                        }
                    }, scope._config.deepWatchData);

                    // Watching on config changing
                    scope.$watch('config', function(newConfig, oldConfig){
                        if (newConfig !== oldConfig){
                            scope._config = angular.extend(defaultConfig, newConfig);
                            scope.api.refresh();
                        }
                    }, true);

                    //subscribe on global events
                    angular.forEach(scope.events, function(eventHandler, event){
                        scope.$on(event, function(e){
                            return eventHandler(e, scope);
                        });
                    });

                    // remove completely when directive is destroyed
                    element.on('$destroy', function () {
                        scope.api.clearElement();
                    });
                }
            };
        }])

        .factory('nvd3Utils', function(){
            return {
                debounce: function(func, wait, immediate) {
                    var timeout;
                    return function() {
                        var context = this, args = arguments;
                        var later = function() {
                            timeout = null;
                            if (!immediate) func.apply(context, args);
                        };
                        var callNow = immediate && !timeout;
                        clearTimeout(timeout);
                        timeout = setTimeout(later, wait);
                        if (callNow) func.apply(context, args);
                    };
                },
                deepExtend: function(dst){
                    var me = this;
                    angular.forEach(arguments, function(obj) {
                        if (obj !== dst) {
                            angular.forEach(obj, function(value, key) {
                                if (dst[key] && dst[key].constructor && dst[key].constructor === Object) {
                                    me.deepExtend(dst[key], value);
                                } else {
                                    dst[key] = value;
                                }
                            });
                        }
                    });
                    return dst;
                }
            };
        });
})();
