/*!
 * angular-datatables - v0.5.1
 * https://github.com/l-lin/angular-datatables
 * License: MIT
 */
(function (window, document, $, angular) {

'use strict';

angular.module('datatables.directive', ['datatables.instances', 'datatables.renderer', 'datatables.options', 'datatables.util'])
    .directive('datatable', dataTable);

/* @ngInject */
function dataTable($q, $http, DTRendererFactory, DTRendererService, DTPropertyUtil) {
    compileDirective.$inject = ['tElm'];
    ControllerDirective.$inject = ['$scope'];
    return {
        restrict: 'A',
        scope: {
            dtOptions: '=',
            dtColumns: '=',
            dtColumnDefs: '=',
            datatable: '@',
            dtInstance: '='
        },
        compile: compileDirective,
        controller: ControllerDirective
    };

    /* @ngInject */
    function compileDirective(tElm) {
        var _staticHTML = tElm[0].innerHTML;

        return function postLink($scope, $elem, iAttrs, ctrl) {
            function handleChanges(newVal, oldVal) {
                if (newVal !== oldVal) {
                    ctrl.render($elem, ctrl.buildOptionsPromise(), _staticHTML);
                }
            }

            // Options can hold heavy data, and other deep/large objects.
            // watchcollection can improve this by only watching shallowly
            var watchFunction = iAttrs.dtDisableDeepWatchers ? '$watchCollection' : '$watch';
            angular.forEach(['dtColumns', 'dtColumnDefs', 'dtOptions'], function(tableDefField) {
                $scope[watchFunction].call($scope, tableDefField, handleChanges, true);
            });
            DTRendererService.showLoading($elem);
            ctrl.render($elem, ctrl.buildOptionsPromise(), _staticHTML);
        };
    }

    /* @ngInject */
    function ControllerDirective($scope) {
        var _dtInstance;
        var vm = this;
        vm.buildOptionsPromise = buildOptionsPromise;
        vm.render = render;

        function buildOptionsPromise() {
            var defer = $q.defer();
            // Build options
            $q.all([
                $q.when($scope.dtOptions),
                $q.when($scope.dtColumns),
                $q.when($scope.dtColumnDefs)
            ]).then(function(results) {
                var dtOptions = results[0],
                    dtColumns = results[1],
                    dtColumnDefs = results[2];
                // Since Angular 1.3, the promise throws a "Maximum call stack size exceeded" when cloning
                // See https://github.com/l-lin/angular-datatables/issues/110
                DTPropertyUtil.deleteProperty(dtOptions, '$promise');
                DTPropertyUtil.deleteProperty(dtColumns, '$promise');
                DTPropertyUtil.deleteProperty(dtColumnDefs, '$promise');
                var options;
                if (angular.isDefined(dtOptions)) {
                    options = {};
                    angular.extend(options, dtOptions);
                    // Set the columns
                    if (angular.isArray(dtColumns)) {
                        options.aoColumns = dtColumns;
                    }

                    // Set the column defs
                    if (angular.isArray(dtColumnDefs)) {
                        options.aoColumnDefs = dtColumnDefs;
                    }

                    // HACK to resolve the language source manually instead of DT
                    // See https://github.com/l-lin/angular-datatables/issues/181
                    if (options.language && options.language.url) {
                        var languageDefer = $q.defer();
                        $http.get(options.language.url).success(function(language) {
                            languageDefer.resolve(language);
                        });
                        options.language = languageDefer.promise;
                    }

                }
                return DTPropertyUtil.resolveObjectPromises(options, ['data', 'aaData', 'fnPromise']);
            }).then(function(options) {
                defer.resolve(options);
            });
            return defer.promise;
        }

        function render($elem, optionsPromise, staticHTML) {
            optionsPromise.then(function(options) {
                DTRendererService.preRender(options);

                var isNgDisplay = $scope.datatable && $scope.datatable === 'ng';
                // Render dataTable
                if (_dtInstance && _dtInstance._renderer) {
                    _dtInstance._renderer.withOptions(options)
                        .render($elem, $scope, staticHTML).then(function(dtInstance) {
                            _dtInstance = dtInstance;
                            _setDTInstance(dtInstance);
                        });
                } else {
                    DTRendererFactory.fromOptions(options, isNgDisplay)
                        .render($elem, $scope, staticHTML).then(function(dtInstance) {
                            _dtInstance = dtInstance;
                            _setDTInstance(dtInstance);
                        });
                }
            });
        }

        function _setDTInstance(dtInstance) {
            if (angular.isFunction($scope.dtInstance)) {
                $scope.dtInstance(dtInstance);
            } else if (angular.isDefined($scope.dtInstance)) {
                $scope.dtInstance = dtInstance;
            }
        }
    }
}
dataTable.$inject = ['$q', '$http', 'DTRendererFactory', 'DTRendererService', 'DTPropertyUtil'];

'use strict';
angular.module('datatables.factory', [])
    .factory('DTOptionsBuilder', dtOptionsBuilder)
    .factory('DTColumnBuilder', dtColumnBuilder)
    .factory('DTColumnDefBuilder', dtColumnDefBuilder)
    .factory('DTLoadingTemplate', dtLoadingTemplate);

/* @ngInject */
function dtOptionsBuilder() {
    /**
     * The wrapped datatables options class
     * @param sAjaxSource the ajax source to fetch the data
     * @param fnPromise the function that returns a promise to fetch the data
     */
    var DTOptions = {
        hasOverrideDom: false,

        /**
         * Add the option to the datatables optoins
         * @param key the key of the option
         * @param value an object or a function of the option
         * @returns {DTOptions} the options
         */
        withOption: function(key, value) {
            if (angular.isString(key)) {
                this[key] = value;
            }
            return this;
        },

        /**
         * Add the Ajax source to the options.
         * This corresponds to the "ajax" option
         * @param ajax the ajax source
         * @returns {DTOptions} the options
         */
        withSource: function(ajax) {
            this.ajax = ajax;
            return this;
        },

        /**
         * Add the ajax data properties.
         * @param sAjaxDataProp the ajax data property
         * @returns {DTOptions} the options
         */
        withDataProp: function(sAjaxDataProp) {
            this.sAjaxDataProp = sAjaxDataProp;
            return this;
        },

        /**
         * Set the server data function.
         * @param fn the function of the server retrieval
         * @returns {DTOptions} the options
         */
        withFnServerData: function(fn) {
            if (!angular.isFunction(fn)) {
                throw new Error('The parameter must be a function');
            }
            this.fnServerData = fn;
            return this;
        },

        /**
         * Set the pagination type.
         * @param sPaginationType the pagination type
         * @returns {DTOptions} the options
         */
        withPaginationType: function(sPaginationType) {
            if (angular.isString(sPaginationType)) {
                this.sPaginationType = sPaginationType;
            } else {
                throw new Error('The pagination type must be provided');
            }
            return this;
        },

        /**
         * Set the language of the datatables
         * @param language the language
         * @returns {DTOptions} the options
         */
        withLanguage: function(language) {
            this.language = language;
            return this;
        },

        /**
         * Set the language source
         * @param languageSource the language source
         * @returns {DTOptions} the options
         */
        withLanguageSource: function(languageSource) {
            return this.withLanguage({
                url: languageSource
            });
        },

        /**
         * Set default number of items per page to display
         * @param iDisplayLength the number of items per page
         * @returns {DTOptions} the options
         */
        withDisplayLength: function(iDisplayLength) {
            this.iDisplayLength = iDisplayLength;
            return this;
        },

        /**
         * Set the promise to fetch the data
         * @param fnPromise the function that returns a promise
         * @returns {DTOptions} the options
         */
        withFnPromise: function(fnPromise) {
            this.fnPromise = fnPromise;
            return this;
        },

        /**
         * Set the Dom of the DataTables.
         * @param dom the dom
         * @returns {DTOptions} the options
         */
        withDOM: function(dom) {
            this.dom = dom;
            return this;
        }
    };

    return {
        /**
         * Create a wrapped datatables options
         * @returns {DTOptions} a wrapped datatables option
         */
        newOptions: function() {
            return Object.create(DTOptions);
        },
        /**
         * Create a wrapped datatables options with the ajax source setted
         * @param ajax the ajax source
         * @returns {DTOptions} a wrapped datatables option
         */
        fromSource: function(ajax) {
            var options = Object.create(DTOptions);
            options.ajax = ajax;
            return options;
        },
        /**
         * Create a wrapped datatables options with the data promise.
         * @param fnPromise the function that returns a promise to fetch the data
         * @returns {DTOptions} a wrapped datatables option
         */
        fromFnPromise: function(fnPromise) {
            var options = Object.create(DTOptions);
            options.fnPromise = fnPromise;
            return options;
        }
    };
}

function dtColumnBuilder() {
    /**
     * The wrapped datatables column
     * @param mData the data to display of the column
     * @param sTitle the sTitle of the column title to display in the DOM
     */
    var DTColumn = {
        /**
         * Add the option of the column
         * @param key the key of the option
         * @param value an object or a function of the option
         * @returns {DTColumn} the wrapped datatables column
         */
        withOption: function(key, value) {
            if (angular.isString(key)) {
                this[key] = value;
            }
            return this;
        },

        /**
         * Set the title of the colum
         * @param sTitle the sTitle of the column
         * @returns {DTColumn} the wrapped datatables column
         */
        withTitle: function(sTitle) {
            this.sTitle = sTitle;
            return this;
        },

        /**
         * Set the CSS class of the column
         * @param sClass the CSS class
         * @returns {DTColumn} the wrapped datatables column
         */
        withClass: function(sClass) {
            this.sClass = sClass;
            return this;
        },

        /**
         * Hide the column
         * @returns {DTColumn} the wrapped datatables column
         */
        notVisible: function() {
            this.bVisible = false;
            return this;
        },

        /**
         * Set the column as not sortable
         * @returns {DTColumn} the wrapped datatables column
         */
        notSortable: function() {
            this.bSortable = false;
            return this;
        },

        /**
         * Render each cell with the given parameter
         * @mRender mRender the function/string to render the data
         * @returns {DTColumn} the wrapped datatables column
         */
        renderWith: function(mRender) {
            this.mRender = mRender;
            return this;
        }
    };

    return {
        /**
         * Create a new wrapped datatables column
         * @param mData the data of the column to display
         * @param sTitle the sTitle of the column title to display in the DOM
         * @returns {DTColumn} the wrapped datatables column
         */
        newColumn: function(mData, sTitle) {
            if (angular.isUndefined(mData)) {
                throw new Error('The parameter "mData" is not defined!');
            }
            var column = Object.create(DTColumn);
            column.mData = mData;
            if (angular.isDefined(sTitle)) {
                column.sTitle = sTitle;
            }
            return column;
        },
        DTColumn: DTColumn
    };
}

/* @ngInject */
function dtColumnDefBuilder(DTColumnBuilder) {
    return {
        newColumnDef: function(targets) {
            if (angular.isUndefined(targets)) {
                throw new Error('The parameter "targets" must be defined! See https://datatables.net/reference/option/columnDefs.targets');
            }
            var column = Object.create(DTColumnBuilder.DTColumn);
            if (angular.isArray(targets)) {
                column.aTargets = targets;
            } else {
                column.aTargets = [targets];
            }
            return column;
        }
    };
}
dtColumnDefBuilder.$inject = ['DTColumnBuilder'];

function dtLoadingTemplate() {
    return {
        html: '<h3 class="dt-loading">Loading...</h3>'
    };
}

'use strict';

angular.module('datatables.instances', ['datatables.util'])
    .factory('DTInstanceFactory', dtInstanceFactory);

function dtInstanceFactory() {
    var DTInstance = {
        reloadData: reloadData,
        changeData: changeData,
        rerender: rerender
    };
    return {
        newDTInstance: newDTInstance,
        copyDTProperties: copyDTProperties
    };

    function newDTInstance(renderer) {
        var dtInstance = Object.create(DTInstance);
        dtInstance._renderer = renderer;
        return dtInstance;
    }

    function copyDTProperties(result, dtInstance) {
        dtInstance.id = result.id;
        dtInstance.DataTable = result.DataTable;
        dtInstance.dataTable = result.dataTable;
    }

    function reloadData(callback, resetPaging) {
        /*jshint validthis:true */
        this._renderer.reloadData(callback, resetPaging);
    }

    function changeData(data) {
        /*jshint validthis:true */
        this._renderer.changeData(data);
    }

    function rerender() {
        /*jshint validthis:true */
        this._renderer.rerender();
    }
}

'use strict';

angular.module('datatables', ['datatables.directive', 'datatables.factory'])
    .run(initAngularDataTables);

/* @ngInject */
function initAngularDataTables() {
    if ($.fn.DataTable.Api) {
        /**
         * Register an API to destroy a DataTable without detaching the tbody so that we can add new data
         * when rendering with the "Angular way".
         */
        $.fn.DataTable.Api.register('ngDestroy()', function(remove) {
            remove = remove || false;

            return this.iterator('table', function(settings) {
                var orig = settings.nTableWrapper.parentNode;
                var classes = settings.oClasses;
                var table = settings.nTable;
                var tbody = settings.nTBody;
                var thead = settings.nTHead;
                var tfoot = settings.nTFoot;
                var jqTable = $(table);
                var jqTbody = $(tbody);
                var jqWrapper = $(settings.nTableWrapper);
                var rows = $.map(settings.aoData, function(r) {
                    return r.nTr;
                });
                var ien;

                // Flag to note that the table is currently being destroyed - no action
                // should be taken
                settings.bDestroying = true;

                // Fire off the destroy callbacks for plug-ins etc
                $.fn.DataTable.ext.internal._fnCallbackFire(settings, 'aoDestroyCallback', 'destroy', [settings]);

                // If not being removed from the document, make all columns visible
                if (!remove) {
                    new $.fn.DataTable.Api(settings).columns().visible(true);
                }

                // Blitz all `DT` namespaced events (these are internal events, the
                // lowercase, `dt` events are user subscribed and they are responsible
                // for removing them
                jqWrapper.unbind('.DT').find(':not(tbody *)').unbind('.DT');
                $(window).unbind('.DT-' + settings.sInstance);

                // When scrolling we had to break the table up - restore it
                if (table !== thead.parentNode) {
                    jqTable.children('thead').detach();
                    jqTable.append(thead);
                }

                if (tfoot && table !== tfoot.parentNode) {
                    jqTable.children('tfoot').detach();
                    jqTable.append(tfoot);
                }

                // Remove the DataTables generated nodes, events and classes
                jqTable.detach();
                jqWrapper.detach();

                settings.aaSorting = [];
                settings.aaSortingFixed = [];
                $.fn.DataTable.ext.internal._fnSortingClasses(settings);

                $(rows).removeClass(settings.asStripeClasses.join(' '));

                $('th, td', thead).removeClass(classes.sSortable + ' ' +
                    classes.sSortableAsc + ' ' + classes.sSortableDesc + ' ' + classes.sSortableNone
                );

                if (settings.bJUI) {
                    $('th span.' + classes.sSortIcon + ', td span.' + classes.sSortIcon, thead).detach();
                    $('th, td', thead).each(function() {
                        var wrapper = $('div.' + classes.sSortJUIWrapper, this);
                        $(this).append(wrapper.contents());
                        wrapper.detach();
                    });
                }

                // -------------------------------------------------------------------------
                // This is the only change with the "destroy()" API (with DT v1.10.1)
                // -------------------------------------------------------------------------
                if (!remove && orig) {
                    // insertBefore acts like appendChild if !arg[1]
                    if (orig.contains(settings.nTableReinsertBefore)) {
                        orig.insertBefore(table, settings.nTableReinsertBefore);
                    } else {
                        orig.appendChild(table);
                    }
                }
                // Add the TR elements back into the table in their original order
                // jqTbody.children().detach();
                // jqTbody.append( rows );
                // -------------------------------------------------------------------------

                // Restore the width of the original table - was read from the style property,
                // so we can restore directly to that
                jqTable
                    .css('width', settings.sDestroyWidth)
                    .removeClass(classes.sTable);

                // If the were originally stripe classes - then we add them back here.
                // Note this is not fool proof (for example if not all rows had stripe
                // classes - but it's a good effort without getting carried away
                ien = settings.asDestroyStripes.length;

                if (ien) {
                    jqTbody.children().each(function(i) {
                        $(this).addClass(settings.asDestroyStripes[i % ien]);
                    });
                }

                /* Remove the settings object from the settings array */
                var idx = $.inArray(settings, $.fn.DataTable.settings);
                if (idx !== -1) {
                    $.fn.DataTable.settings.splice(idx, 1);
                }
            });
        });
    }
}

'use strict';
angular.module('datatables.options', [])
    .constant('DT_DEFAULT_OPTIONS', {
        // Default dom
        dom: 'lfrtip',
        // Default ajax properties. See http://legacy.datatables.net/usage/options#sAjaxDataProp
        sAjaxDataProp: '',
        // Set default columns (used when none are provided)
        aoColumns: []
    })
    .service('DTDefaultOptions', dtDefaultOptions);

function dtDefaultOptions() {
    var options = {
        bootstrapOptions: {},
        setLanguageSource: setLanguageSource,
        setLanguage: setLanguage,
        setDisplayLength: setDisplayLength,
        setBootstrapOptions: setBootstrapOptions
    };

    return options;

    /**
     * Set the default language source for all datatables
     * @param sLanguageSource the language source
     * @returns {DTDefaultOptions} the default option config
     */
    function setLanguageSource(sLanguageSource) {
        // HACK to resolve the language source manually instead of DT
        // See https://github.com/l-lin/angular-datatables/issues/356
        $.ajax({
            dataType: 'json',
            url: sLanguageSource,
            success: function(json) {
                $.extend(true, $.fn.dataTable.defaults, {
                    oLanguage: json
                });
            }
        });
        return options;
    }

    /**
     * Set the language for all datatables
     * @param oLanguage the language
     * @returns {DTDefaultOptions} the default option config
     */
    function setLanguage(oLanguage) {
        $.extend(true, $.fn.dataTable.defaults, {
            oLanguage: oLanguage
        });
        return options;
    }

    /**
     * Set the default number of items to display for all datatables
     * @param iDisplayLength the number of items to display
     * @returns {DTDefaultOptions} the default option config
     */
    function setDisplayLength(iDisplayLength) {
        $.extend($.fn.dataTable.defaults, {
            iDisplayLength: iDisplayLength
        });
        return options;
    }

    /**
     * Set the default options to be use for Bootstrap integration.
     * See https://github.com/l-lin/angular-datatables/blob/dev/src/angular-datatables.bootstrap.options.js to check
     * what default options Angular DataTables is using.
     * @param oBootstrapOptions an object containing the default options for Bootstreap integration
     * @returns {DTDefaultOptions} the default option config
     */
    function setBootstrapOptions(oBootstrapOptions) {
        options.bootstrapOptions = oBootstrapOptions;
        return options;
    }
}

'use strict';
angular.module('datatables.renderer', ['datatables.instances', 'datatables.factory', 'datatables.options', 'datatables.instances'])
    .factory('DTRendererService', dtRendererService)
    .factory('DTRenderer', dtRenderer)
    .factory('DTDefaultRenderer', dtDefaultRenderer)
    .factory('DTNGRenderer', dtNGRenderer)
    .factory('DTPromiseRenderer', dtPromiseRenderer)
    .factory('DTAjaxRenderer', dtAjaxRenderer)
    .factory('DTRendererFactory', dtRendererFactory);

/* @ngInject */
function dtRendererService(DTLoadingTemplate) {
    var plugins = [];
    var rendererService = {
        showLoading: showLoading,
        hideLoading: hideLoading,
        renderDataTable: renderDataTable,
        hideLoadingAndRenderDataTable: hideLoadingAndRenderDataTable,
        registerPlugin: registerPlugin,
        postRender: postRender,
        preRender: preRender
    };
    return rendererService;

    function showLoading($elem) {
        var $loading = angular.element(DTLoadingTemplate.html);
        $elem.after($loading);
        $elem.hide();
        $loading.show();
    }

    function hideLoading($elem) {
        $elem.show();
        $elem.next().remove();
    }

    function renderDataTable($elem, options) {
        var dtId = '#' + $elem.attr('id');
        if ($.fn.dataTable.isDataTable(dtId) && angular.isObject(options)) {
            options.destroy = true;
        }
        // See http://datatables.net/manual/api#Accessing-the-API to understand the difference between DataTable and dataTable
        var DT = $elem.DataTable(options);
        var dt = $elem.dataTable();

        var result = {
            id: $elem.attr('id'),
            DataTable: DT,
            dataTable: dt
        };

        postRender(options, result);

        return result;
    }

    function hideLoadingAndRenderDataTable($elem, options) {
        rendererService.hideLoading($elem);
        return rendererService.renderDataTable($elem, options);
    }

    function registerPlugin(plugin) {
        plugins.push(plugin);
    }

    function postRender(options, result) {
        angular.forEach(plugins, function(plugin) {
            if (angular.isFunction(plugin.postRender)) {
                plugin.postRender(options, result);
            }
        });
    }

    function preRender(options) {
        angular.forEach(plugins, function(plugin) {
            if (angular.isFunction(plugin.preRender)) {
                plugin.preRender(options);
            }
        });
    }
}
dtRendererService.$inject = ['DTLoadingTemplate'];

function dtRenderer() {
    return {
        withOptions: function(options) {
            this.options = options;
            return this;
        }
    };
}

/* @ngInject */
function dtDefaultRenderer($q, DTRenderer, DTRendererService, DTInstanceFactory) {
    return {
        create: create
    };

    function create(options) {
        var _oTable;
        var _$elem;
        var renderer = Object.create(DTRenderer);
        renderer.name = 'DTDefaultRenderer';
        renderer.options = options;
        renderer.render = render;
        renderer.reloadData = reloadData;
        renderer.changeData = changeData;
        renderer.rerender = rerender;

        function render($elem) {
            _$elem = $elem;
            var dtInstance = DTInstanceFactory.newDTInstance(renderer);
            var result = DTRendererService.hideLoadingAndRenderDataTable($elem, renderer.options);
            _oTable = result.DataTable;
            DTInstanceFactory.copyDTProperties(result, dtInstance);
            return $q.when(dtInstance);
        }

        function reloadData() {
            // Do nothing
        }

        function changeData() {
            // Do nothing
        }

        function rerender() {
            _oTable.destroy();
            DTRendererService.showLoading(_$elem);
            render(_$elem);
        }
        return renderer;
    }
}
dtDefaultRenderer.$inject = ['$q', 'DTRenderer', 'DTRendererService', 'DTInstanceFactory'];

/* @ngInject */
function dtNGRenderer($log, $q, $compile, $timeout, DTRenderer, DTRendererService, DTInstanceFactory) {
    /**
     * Renderer for displaying the Angular way
     * @param options
     * @returns {{options: *}} the renderer
     * @constructor
     */
    return {
        create: create
    };

    function create(options) {
        var _staticHTML;
        var _oTable;
        var _$elem;
        var _parentScope;
        var _newParentScope;
        var dtInstance;
        var renderer = Object.create(DTRenderer);
        renderer.name = 'DTNGRenderer';
        renderer.options = options;
        renderer.render = render;
        renderer.reloadData = reloadData;
        renderer.changeData = changeData;
        renderer.rerender = rerender;
        return renderer;

        function render($elem, $scope, staticHTML) {
            _staticHTML = staticHTML;
            _$elem = $elem;
            _parentScope = $scope.$parent;
            dtInstance = DTInstanceFactory.newDTInstance(renderer);

            var defer = $q.defer();
            var _expression = $elem.find('tbody').html();
            // Find the resources from the comment <!-- ngRepeat: item in items --> displayed by angular in the DOM
            // This regexp is inspired by the one used in the "ngRepeat" directive
            var _match = _expression.match(/^\s*.+?\s+in\s+(\S*)\s*/m);

            if (!_match) {
                throw new Error('Expected expression in form of "_item_ in _collection_[ track by _id_]" but got "{0}".', _expression);
            }
            var _ngRepeatAttr = _match[1];

            var _alreadyRendered = false;

            _parentScope.$watchCollection(_ngRepeatAttr, function() {
                if (_oTable && _alreadyRendered) {
                    _destroyAndCompile();
                }
                $timeout(function() {
                    _alreadyRendered = true;
                    var result = DTRendererService.hideLoadingAndRenderDataTable(_$elem, renderer.options);
                    _oTable = result.DataTable;
                    DTInstanceFactory.copyDTProperties(result, dtInstance);
                    defer.resolve(dtInstance);
                }, 0, false);
            }, true);
            return defer.promise;
        }

        function reloadData() {
            $log.warn('The Angular Renderer does not support reloading data. You need to do it directly on your model');
        }

        function changeData() {
            $log.warn('The Angular Renderer does not support changing the data. You need to change your model directly.');
        }

        function rerender() {
            _destroyAndCompile();
            DTRendererService.showLoading(_$elem);
            $timeout(function() {
                var result = DTRendererService.hideLoadingAndRenderDataTable(_$elem, renderer.options);
                _oTable = result.DataTable;
                DTInstanceFactory.copyDTProperties(result, dtInstance);
            }, 0, false);
        }

        function _destroyAndCompile() {
            if (_newParentScope) {
                _newParentScope.$destroy();
            }
            _oTable.ngDestroy();
            // Re-compile because we lost the angular binding to the existing data
            _$elem.html(_staticHTML);
            _newParentScope = _parentScope.$new();
            $compile(_$elem.contents())(_newParentScope);
        }
    }
}
dtNGRenderer.$inject = ['$log', '$q', '$compile', '$timeout', 'DTRenderer', 'DTRendererService', 'DTInstanceFactory'];

/* @ngInject */
function dtPromiseRenderer($q, $timeout, $log, DTRenderer, DTRendererService, DTInstanceFactory) {
    /**
     * Renderer for displaying with a promise
     * @param options the options
     * @returns {{options: *}} the renderer
     * @constructor
     */
    return {
        create: create
    };

    function create(options) {
        var _oTable;
        var _loadedPromise = null;
        var _$elem;
        var _$scope;

        var dtInstance;
        var renderer = Object.create(DTRenderer);
        renderer.name = 'DTPromiseRenderer';
        renderer.options = options;
        renderer.render = render;
        renderer.reloadData = reloadData;
        renderer.changeData = changeData;
        renderer.rerender = rerender;
        return renderer;

        function render($elem, $scope) {
            var defer = $q.defer();
            dtInstance = DTInstanceFactory.newDTInstance(renderer);
            _$elem = $elem;
            _$scope = $scope;
            _resolve(renderer.options.fnPromise, DTRendererService.renderDataTable).then(function(result) {
                _oTable = result.DataTable;
                DTInstanceFactory.copyDTProperties(result, dtInstance);
                defer.resolve(dtInstance);
            });
            return defer.promise;
        }

        function reloadData(callback, resetPaging) {
            var previousPage = _oTable && _oTable.page() ? _oTable.page() : 0;
            if (angular.isFunction(renderer.options.fnPromise)) {
                DTRendererService.showLoading(_$elem);
                _resolve(renderer.options.fnPromise, _redrawRows).then(function(result) {
                    if (angular.isFunction(callback)) {
                        callback(result.DataTable.data());
                    }
                    if (resetPaging === false) {
                        result.DataTable.page(previousPage).draw(false);
                    }
                });
            } else {
                $log.warn('In order to use the reloadData functionality with a Promise renderer, you need to provide a function that returns a promise.');
            }
        }

        function changeData(fnPromise) {
            renderer.options.fnPromise = fnPromise;
            // We also need to set the $scope.dtOptions, otherwise, when we change the columns, it will revert to the old data
            // See https://github.com/l-lin/angular-datatables/issues/359
            _$scope.dtOptions.fnPromise = fnPromise;
            DTRendererService.showLoading(_$elem);
            _resolve(renderer.options.fnPromise, _redrawRows);
        }

        function rerender() {
            _oTable.destroy();
            DTRendererService.showLoading(_$elem);
            render(_$elem, _$scope);
        }

        function _resolve(fnPromise, callback) {
            var defer = $q.defer();
            if (angular.isUndefined(fnPromise)) {
                throw new Error('You must provide a promise or a function that returns a promise!');
            }
            if (_loadedPromise) {
                _loadedPromise.then(function(){
                    defer.resolve(_startLoading(fnPromise, callback));
                });
            } else {
                defer.resolve(_startLoading(fnPromise, callback));
            }
            return defer.promise;
        }

        function _startLoading(fnPromise, callback) {
            var defer = $q.defer();
            if (angular.isFunction(fnPromise)) {
                _loadedPromise = fnPromise();
            } else {
                _loadedPromise = fnPromise;
            }
            _loadedPromise.then(function(result) {
                var data = result;
                // In case the data is nested in an object
                if (renderer.options.sAjaxDataProp) {
                    var properties = renderer.options.sAjaxDataProp.split('.');
                    while (properties.length) {
                        var property = properties.shift();
                        if (property in data) {
                            data = data[property];
                        }
                    }
                }
                _loadedPromise = null;
                defer.resolve(_doRender(renderer.options, _$elem, data, callback));
            });
            return defer.promise;
        }

        function _doRender(options, $elem, data, callback) {
            var defer = $q.defer();
            // Since Angular 1.3, the promise renderer is throwing "Maximum call stack size exceeded"
            // By removing the $promise attribute, we avoid an infinite loop when jquery is cloning the data
            // See https://github.com/l-lin/angular-datatables/issues/110
            delete data.$promise;
            options.aaData = data;
            // Add $timeout to be sure that angular has finished rendering before calling datatables
            $timeout(function() {
                DTRendererService.hideLoading($elem);
                // Set it to true in order to be able to redraw the dataTable
                options.bDestroy = true;
                defer.resolve(callback($elem, options));
            }, 0, false);
            return defer.promise;
        }

        function _redrawRows($elem, options) {
            _oTable.clear();
            _oTable.rows.add(options.aaData).draw(options.redraw);
            return {
                id: dtInstance.id,
                DataTable: dtInstance.DataTable,
                dataTable: dtInstance.dataTable
            };
        }
    }
}
dtPromiseRenderer.$inject = ['$q', '$timeout', '$log', 'DTRenderer', 'DTRendererService', 'DTInstanceFactory'];

/* @ngInject */
function dtAjaxRenderer($q, $timeout, DTRenderer, DTRendererService, DT_DEFAULT_OPTIONS, DTInstanceFactory) {
    /**
     * Renderer for displaying with Ajax
     * @param options the options
     * @returns {{options: *}} the renderer
     * @constructor
     */
    return {
        create: create
    };

    function create(options) {
        var _oTable;
        var _$elem;
        var _$scope;
        var renderer = Object.create(DTRenderer);
        renderer.name = 'DTAjaxRenderer';
        renderer.options = options;
        renderer.render = render;
        renderer.reloadData = reloadData;
        renderer.changeData = changeData;
        renderer.rerender = rerender;
        return renderer;

        function render($elem, $scope) {
            _$elem = $elem;
            _$scope = $scope;
            var defer = $q.defer();
            var dtInstance = DTInstanceFactory.newDTInstance(renderer);
            // Define default values in case it is an ajax datatables
            if (angular.isUndefined(renderer.options.sAjaxDataProp)) {
                renderer.options.sAjaxDataProp = DT_DEFAULT_OPTIONS.sAjaxDataProp;
            }
            if (angular.isUndefined(renderer.options.aoColumns)) {
                renderer.options.aoColumns = DT_DEFAULT_OPTIONS.aoColumns;
            }
            _doRender(renderer.options, $elem).then(function(result) {
                _oTable = result.DataTable;
                DTInstanceFactory.copyDTProperties(result, dtInstance);
                defer.resolve(dtInstance);
            });
            return defer.promise;
        }

        function reloadData(callback, resetPaging) {
            if (_oTable) {
                _oTable.ajax.reload(callback, resetPaging);
            }
        }

        function changeData(ajax) {
            renderer.options.ajax = ajax;
            // We also need to set the $scope.dtOptions, otherwise, when we change the columns, it will revert to the old data
            // See https://github.com/l-lin/angular-datatables/issues/359
            _$scope.dtOptions.ajax = ajax;
        }

        function rerender() {
            render(_$elem, _$scope);
        }

        function _doRender(options, $elem) {
            var defer = $q.defer();
            // Destroy the table if it exists in order to be able to redraw the dataTable
            options.bDestroy = true;
            if (_oTable) {
                _oTable.destroy();
                DTRendererService.showLoading(_$elem);
                // Empty in case of columns change
                $elem.empty();
            }
            DTRendererService.hideLoading($elem);
            // Condition to refresh the dataTable
            if (_shouldDeferRender(options)) {
                $timeout(function() {
                    defer.resolve(DTRendererService.renderDataTable($elem, options));
                }, 0, false);
            } else {
                defer.resolve(DTRendererService.renderDataTable($elem, options));
            }
            return defer.promise;
        }
        // See https://github.com/l-lin/angular-datatables/issues/147
        function _shouldDeferRender(options) {
            if (angular.isDefined(options) && angular.isDefined(options.dom)) {
                // S for scroller plugin
                return options.dom.indexOf('S') >= 0;
            }
            return false;
        }
    }
}
dtAjaxRenderer.$inject = ['$q', '$timeout', 'DTRenderer', 'DTRendererService', 'DT_DEFAULT_OPTIONS', 'DTInstanceFactory'];

/* @ngInject */
function dtRendererFactory(DTDefaultRenderer, DTNGRenderer, DTPromiseRenderer, DTAjaxRenderer) {
    return {
        fromOptions: fromOptions
    };

    function fromOptions(options, isNgDisplay){
        if (isNgDisplay) {
            if (options && options.serverSide) {
                throw new Error('You cannot use server side processing along with the Angular renderer!');
            }
            return DTNGRenderer.create(options);
        }
        if (angular.isDefined(options)) {
            if (angular.isDefined(options.fnPromise) && options.fnPromise !== null) {
                if (options.serverSide) {
                    throw new Error('You cannot use server side processing along with the Promise renderer!');
                }
                return DTPromiseRenderer.create(options);
            }
            if (angular.isDefined(options.ajax) && options.ajax !== null ||
                angular.isDefined(options.ajax) && options.ajax !== null) {
                return DTAjaxRenderer.create(options);
            }
            return DTDefaultRenderer.create(options);
        }
        return DTDefaultRenderer.create();
    }
}
dtRendererFactory.$inject = ['DTDefaultRenderer', 'DTNGRenderer', 'DTPromiseRenderer', 'DTAjaxRenderer'];

'use strict';

angular.module('datatables.util', [])
    .factory('DTPropertyUtil', dtPropertyUtil);

/* @ngInject */
function dtPropertyUtil($q) {
    return {
        overrideProperties: overrideProperties,
        deleteProperty: deleteProperty,
        resolveObjectPromises: resolveObjectPromises,
        resolveArrayPromises: resolveArrayPromises
    };

    /**
     * Overrides the source property with the given target properties.
     * Source is not written. It's making a fresh copy of it in order to ensure that we do not change the parameters.
     * @param source the source properties to override
     * @param target the target properties
     * @returns {*} the object overrided
     */
    function overrideProperties(source, target) {
        var result = angular.copy(source);

        if (angular.isUndefined(result) || result === null) {
            result = {};
        }
        if (angular.isUndefined(target) || target === null) {
            return result;
        }
        if (angular.isObject(target)) {
            for (var prop in target) {
                if (target.hasOwnProperty(prop)) {
                    result[prop] = overrideProperties(result[prop], target[prop]);
                }
            }
        } else {
            result = angular.copy(target);
        }
        return result;
    }

    /**
     * Delete the property from the given object
     * @param obj the object
     * @param propertyName the property name
     */
    function deleteProperty(obj, propertyName) {
        if (angular.isObject(obj)) {
            delete obj[propertyName];
        }
    }

    /**
     * Resolve any promises from a given object if there are any.
     * @param obj the object
     * @param excludedPropertiesName the list of properties to ignore
     * @returns {promise} the promise that the object attributes promises are all resolved
     */
    function resolveObjectPromises(obj, excludedPropertiesName) {
        var defer = $q.defer(),
            promises = [],
            resolvedObj = {},
            excludedProp = excludedPropertiesName || [];
        if (!angular.isObject(obj) || angular.isArray(obj)) {
            defer.resolve(obj);
        } else {
            resolvedObj = angular.extend(resolvedObj, obj);
            for (var prop in resolvedObj) {
                if (resolvedObj.hasOwnProperty(prop) && $.inArray(prop, excludedProp) === -1) {
                    if (angular.isArray(resolvedObj[prop])) {
                        promises.push(resolveArrayPromises(resolvedObj[prop]));
                    } else {
                        promises.push($q.when(resolvedObj[prop]));
                    }
                }
            }
            $q.all(promises).then(function(result) {
                var index = 0;
                for (var prop in resolvedObj) {
                    if (resolvedObj.hasOwnProperty(prop) && $.inArray(prop, excludedProp) === -1) {
                        resolvedObj[prop] = result[index++];
                    }
                }
                defer.resolve(resolvedObj);
            });
        }
        return defer.promise;
    }

    /**
     * Resolve the given array promises
     * @param array the array containing promise or not
     * @returns {promise} the promise that the array contains a list of objects/values promises that are resolved
     */
    function resolveArrayPromises(array) {
        var defer = $q.defer(),
            promises = [],
            resolveArray = [];
        if (!angular.isArray(array)) {
            defer.resolve(array);
        } else {
            angular.forEach(array, function(item) {
                if (angular.isObject(item)) {
                    promises.push(resolveObjectPromises(item));
                } else {
                    promises.push($q.when(item));
                }
            });
            $q.all(promises).then(function(result) {
                angular.forEach(result, function(item) {
                    resolveArray.push(item);
                });
                defer.resolve(resolveArray);
            });
        }
        return defer.promise;
    }
}
dtPropertyUtil.$inject = ['$q'];


})(window, document, jQuery, angular);
