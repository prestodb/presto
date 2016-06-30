/*!
 * angular-datatables - v0.5.0
 * https://github.com/l-lin/angular-datatables
 * License: MIT
 */
(function (window, document, $, angular) {

'use strict';
angular.module('datatables.bootstrap.colvis', ['datatables.bootstrap.options', 'datatables.util'])
    .service('DTBootstrapColVis', dtBootstrapColVis);

/* @ngInject */
function dtBootstrapColVis(DTPropertyUtil, DTBootstrapDefaultOptions) {
    var _initializedColVis = false;
    return {
        integrate: integrate,
        deIntegrate: deIntegrate
    };

    function integrate(addDrawCallbackFunction, bootstrapOptions) {
        if (!_initializedColVis) {
            var colVisProperties = DTPropertyUtil.overrideProperties(
                DTBootstrapDefaultOptions.getOptions().ColVis,
                bootstrapOptions ? bootstrapOptions.ColVis : null
            );
            /* ColVis Bootstrap compatibility */
            if ($.fn.DataTable.ColVis) {
                addDrawCallbackFunction(function() {
                    $('.ColVis_MasterButton').attr('class', 'ColVis_MasterButton ' + colVisProperties.classes.masterButton);
                    $('.ColVis_Button').removeClass('ColVis_Button');
                });
            }

            _initializedColVis = true;
        }
    }

    function deIntegrate() {
        if (_initializedColVis && $.fn.DataTable.ColVis) {
            _initializedColVis = false;
        }
    }
}
dtBootstrapColVis.$inject = ['DTPropertyUtil', 'DTBootstrapDefaultOptions'];

'use strict';

// See http://getbootstrap.com
angular.module('datatables.bootstrap', [
        'datatables.bootstrap.options',
        'datatables.bootstrap.tabletools',
        'datatables.bootstrap.colvis'
    ])
    .config(dtBootstrapConfig)
    .run(initBootstrapPlugin)
    .service('DTBootstrap', dtBootstrap);

/* @ngInject */
function dtBootstrapConfig($provide) {
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
            options.withBootstrap = withBootstrap;
            options.withBootstrapOptions = withBootstrapOptions;
            return options;

            /**
             * Add bootstrap compatibility
             * @returns {DTOptions} the options
             */
            function withBootstrap() {
                options.hasBootstrap = true;
                // Override page button active CSS class
                if (angular.isObject(options.oClasses)) {
                    options.oClasses.sPageButtonActive = 'active';
                } else {
                    options.oClasses = {
                        sPageButtonActive: 'active'
                    };
                }
                return options;
            }

            /**
             * Add bootstrap options
             * @param bootstrapOptions the bootstrap options
             * @returns {DTOptions} the options
             */
            function withBootstrapOptions(bootstrapOptions) {
                options.bootstrap = bootstrapOptions;
                return options;
            }
        }
    }
    dtOptionsBuilderDecorator.$inject = ['$delegate'];
}
dtBootstrapConfig.$inject = ['$provide'];

/* @ngInject */
function initBootstrapPlugin(DTRendererService, DTBootstrap) {
    var columnFilterPlugin = {
        preRender: preRender
    };
    DTRendererService.registerPlugin(columnFilterPlugin);

    function preRender(options) {
        // Integrate bootstrap (or not)
        if (options && options.hasBootstrap) {
            DTBootstrap.integrate(options);
        } else {
            DTBootstrap.deIntegrate();
        }
    }
}
initBootstrapPlugin.$inject = ['DTRendererService', 'DTBootstrap'];

/**
 * Source: https://editor.datatables.net/release/DataTables/extras/Editor/examples/bootstrap.html
 */
/* @ngInject */
function dtBootstrap(DTBootstrapTableTools, DTBootstrapColVis, DTBootstrapDefaultOptions, DTPropertyUtil, DT_DEFAULT_OPTIONS) {
    var _initialized = false,
        _drawCallbackFunctionList = [],
        _savedFn = {};

    return {
        integrate: integrate,
        deIntegrate: deIntegrate
    };

    function _saveFnToBeOverrided() {
        _savedFn.oStdClasses = angular.copy($.fn.dataTableExt.oStdClasses);
        _savedFn.fnPagingInfo = $.fn.dataTableExt.oApi.fnPagingInfo;
        _savedFn.renderer = angular.copy($.fn.DataTable.ext.renderer);
        if ($.fn.DataTable.TableTools) {
            _savedFn.TableTools = {
                classes: angular.copy($.fn.DataTable.TableTools.classes),
                oTags: angular.copy($.fn.DataTable.TableTools.DEFAULTS.oTags)
            };
        }
    }

    function _revertToDTFn() {
        $.extend($.fn.dataTableExt.oStdClasses, _savedFn.oStdClasses);
        $.fn.dataTableExt.oApi.fnPagingInfo = _savedFn.fnPagingInfo;
        $.extend(true, $.fn.DataTable.ext.renderer, _savedFn.renderer);
    }

    function _overrideClasses() {
        /* Default class modification */
        $.extend($.fn.dataTableExt.oStdClasses, {
            'sWrapper': 'dataTables_wrapper form-inline',
            'sFilterInput': 'form-control input-sm',
            'sLengthSelect': 'form-control input-sm',
            'sFilter': 'dataTables_filter',
            'sLength': 'dataTables_length'
        });
    }

    function _overridePagingInfo() {
        /* API method to get paging information */
        $.fn.dataTableExt.oApi.fnPagingInfo = function(oSettings) {
            return {
                'iStart': oSettings._iDisplayStart,
                'iEnd': oSettings.fnDisplayEnd(),
                'iLength': oSettings._iDisplayLength,
                'iTotal': oSettings.fnRecordsTotal(),
                'iFilteredTotal': oSettings.fnRecordsDisplay(),
                'iPage': oSettings._iDisplayLength === -1 ? 0 : Math.ceil(oSettings._iDisplayStart / oSettings._iDisplayLength),
                'iTotalPages': oSettings._iDisplayLength === -1 ? 0 : Math.ceil(oSettings.fnRecordsDisplay() / oSettings._iDisplayLength)
            };
        };
    }

    function _overridePagination(bootstrapOptions) {
        // Note: Copy paste with some changes from DataTables v1.10.1 source code
        $.extend(true, $.fn.DataTable.ext.renderer, {
            pageButton: {
                _: function(settings, host, idx, buttons, page, pages) {
                    var classes = settings.oClasses;
                    var lang = settings.language ? settings.language.oPaginate : settings.oLanguage.oPaginate;
                    var btnDisplay, btnClass, counter = 0;
                    var paginationClasses = DTPropertyUtil.overrideProperties(
                        DTBootstrapDefaultOptions.getOptions().pagination,
                        bootstrapOptions ? bootstrapOptions.pagination : null
                    );
                    var $paginationContainer = $('<ul></ul>', {
                        'class': paginationClasses.classes.ul
                    });

                    var attach = function(container, buttons) {
                        var i, ien, node, button;
                        var clickHandler = function(e) {
                            e.preventDefault();
                            // IMPORTANT: Reference to internal functions of DT. It might change between versions
                            $.fn.DataTable.ext.internal._fnPageChange(settings, e.data.action, true);
                        };


                        for (i = 0, ien = buttons.length; i < ien; i++) {
                            button = buttons[i];

                            if ($.isArray(button)) {
                                // Override DT element
                                button.DT_el = 'li';
                                var inner = $('<' + (button.DT_el || 'div') + '/>')
                                    .appendTo($paginationContainer);
                                attach(inner, button);
                            } else {
                                btnDisplay = '';
                                btnClass = '';
                                var $paginationBtn = $('<li></li>'),
                                    isDisabled;

                                switch (button) {
                                    case 'ellipsis':
                                        $paginationContainer.append('<li class="disabled"><a href="#" onClick="event.preventDefault()">&hellip;</a></li>');
                                        break;

                                    case 'first':
                                        btnDisplay = lang.sFirst;
                                        btnClass = button;
                                        if (page <= 0) {
                                            $paginationBtn.addClass(classes.sPageButtonDisabled);
                                            isDisabled = true;
                                        }
                                        break;

                                    case 'previous':
                                        btnDisplay = lang.sPrevious;
                                        btnClass = button;
                                        if (page <= 0) {
                                            $paginationBtn.addClass(classes.sPageButtonDisabled);
                                            isDisabled = true;
                                        }
                                        break;

                                    case 'next':
                                        btnDisplay = lang.sNext;
                                        btnClass = button;
                                        if (page >= pages - 1) {
                                            $paginationBtn.addClass(classes.sPageButtonDisabled);
                                            isDisabled = true;
                                        }
                                        break;

                                    case 'last':
                                        btnDisplay = lang.sLast;
                                        btnClass = button;
                                        if (page >= pages - 1) {
                                            $paginationBtn.addClass(classes.sPageButtonDisabled);
                                            isDisabled = true;
                                        }
                                        break;

                                    default:
                                        btnDisplay = button + 1;
                                        btnClass = '';
                                        if (page === button) {
                                            $paginationBtn.addClass(classes.sPageButtonActive);
                                        }
                                        break;
                                }

                                if (btnDisplay) {
                                    $paginationBtn.appendTo($paginationContainer);
                                    node = $('<a>', {
                                            'href': '#',
                                            'class': btnClass,
                                            'aria-controls': settings.sTableId,
                                            'data-dt-idx': counter,
                                            'tabindex': settings.iTabIndex,
                                            'id': idx === 0 && typeof button === 'string' ?
                                                settings.sTableId + '_' + button : null
                                        })
                                        .html(btnDisplay)
                                        .appendTo($paginationBtn);

                                    // IMPORTANT: Reference to internal functions of DT. It might change between versions
                                    $.fn.DataTable.ext.internal._fnBindAction(
                                        node, {
                                            action: button
                                        }, clickHandler
                                    );

                                    counter++;
                                }
                            }
                        }
                    };

                    // IE9 throws an 'unknown error' if document.activeElement is used
                    // inside an iframe or frame. Try / catch the error. Not good for
                    // accessibility, but neither are frames.
                    try {
                        // Because this approach is destroying and recreating the paging
                        // elements, focus is lost on the select button which is bad for
                        // accessibility. So we want to restore focus once the draw has
                        // completed
                        var activeEl = $(document.activeElement).data('dt-idx');

                        // Add <ul> to the pagination
                        var container = $(host).empty();
                        $paginationContainer.appendTo(container);
                        attach(container, buttons);

                        if (activeEl !== null) {
                            $(host).find('[data-dt-idx=' + activeEl + ']').focus();
                        }
                    } catch (e) {}
                }
            }
        });
    }

    function _addDrawCallbackFunction(fn) {
        if (angular.isFunction(fn)) {
            _drawCallbackFunctionList.push(fn);
        }
    }

    function _init(bootstrapOptions) {
        if (!_initialized) {
            _saveFnToBeOverrided();
            _overrideClasses();
            _overridePagingInfo();
            _overridePagination(bootstrapOptions);

            _addDrawCallbackFunction(function() {
                $('div.dataTables_filter').find('input').addClass('form-control');
                $('div.dataTables_length').find('select').addClass('form-control');
            });

            _initialized = true;
        }
    }

    function _setDom(options) {
            if (!options.dom || options.dom === DT_DEFAULT_OPTIONS.dom) {
                return DTBootstrapDefaultOptions.getOptions().dom;
            }
            return options.dom;
        }
        /**
         * Integrate Bootstrap
         * @param options the datatables options
         */
    function integrate(options) {
        _init(options.bootstrap);
        DTBootstrapTableTools.integrate(options.bootstrap);
        DTBootstrapColVis.integrate(_addDrawCallbackFunction, options.bootstrap);

        options.dom = _setDom(options);
        if (angular.isUndefined(options.fnDrawCallback)) {
            // Call every drawcallback functions
            options.fnDrawCallback = function() {
                for (var index = 0; index < _drawCallbackFunctionList.length; index++) {
                    _drawCallbackFunctionList[index]();
                }
            };
        }
    }

    function deIntegrate() {
        if (_initialized) {
            _revertToDTFn();
            DTBootstrapTableTools.deIntegrate();
            DTBootstrapColVis.deIntegrate();
            _initialized = false;
        }
    }
}
dtBootstrap.$inject = ['DTBootstrapTableTools', 'DTBootstrapColVis', 'DTBootstrapDefaultOptions', 'DTPropertyUtil', 'DT_DEFAULT_OPTIONS'];

'use strict';
angular.module('datatables.bootstrap.options', ['datatables.options', 'datatables.util'])
    .constant('DT_BOOTSTRAP_DEFAULT_OPTIONS', {
        TableTools: {
            classes: {
                container: 'DTTT btn-group',
                buttons: {
                    normal: 'btn btn-default',
                    disabled: 'disabled'
                },
                collection: {
                    container: 'DTTT_dropdown dropdown-menu',
                    buttons: {
                        normal: '',
                        disabled: 'disabled'
                    }
                },
                print: {
                    info: 'DTTT_print_info modal'
                },
                select: {
                    row: 'active'
                }
            },
            DEFAULTS: {
                oTags: {
                    collection: {
                        container: 'ul',
                        button: 'li',
                        liner: 'a'
                    }
                }
            }
        },
        ColVis: {
            classes: {
                masterButton: 'btn btn-default'
            }
        },
        pagination: {
            classes: {
                ul: 'pagination'
            }
        },
        dom: '<\'row\'<\'col-xs-6\'l><\'col-xs-6\'f>r>t<\'row\'<\'col-xs-6\'i><\'col-xs-6\'p>>'
    })
    .factory('DTBootstrapDefaultOptions', dtBootstrapDefaultOptions);

/* @ngInject */
function dtBootstrapDefaultOptions(DTDefaultOptions, DTPropertyUtil, DT_BOOTSTRAP_DEFAULT_OPTIONS) {
    return {
        getOptions: getOptions
    };
    /**
     * Get the default options for bootstrap integration
     * @returns {*} the bootstrap default options
     */
    function getOptions() {
        return DTPropertyUtil.overrideProperties(DT_BOOTSTRAP_DEFAULT_OPTIONS, DTDefaultOptions.bootstrapOptions);
    }
}
dtBootstrapDefaultOptions.$inject = ['DTDefaultOptions', 'DTPropertyUtil', 'DT_BOOTSTRAP_DEFAULT_OPTIONS'];

'use strict';

angular.module('datatables.bootstrap.tabletools', ['datatables.bootstrap.options', 'datatables.util'])
    .service('DTBootstrapTableTools', dtBootstrapTableTools);

/* @ngInject */
function dtBootstrapTableTools(DTPropertyUtil, DTBootstrapDefaultOptions) {
    var _initializedTableTools = false,
        _savedFn = {};

    return {
        integrate: integrate,
        deIntegrate: deIntegrate
    };

    function integrate(bootstrapOptions) {
        if (!_initializedTableTools) {
            _saveFnToBeOverrided();

            /*
             * TableTools Bootstrap compatibility
             * Required TableTools 2.1+
             */
            if ($.fn.DataTable.TableTools) {
                var tableToolsOptions = DTPropertyUtil.overrideProperties(
                    DTBootstrapDefaultOptions.getOptions().TableTools,
                    bootstrapOptions ? bootstrapOptions.TableTools : null
                );
                // Set the classes that TableTools uses to something suitable for Bootstrap
                $.extend(true, $.fn.DataTable.TableTools.classes, tableToolsOptions.classes);

                // Have the collection use a bootstrap compatible dropdown
                $.extend(true, $.fn.DataTable.TableTools.DEFAULTS.oTags, tableToolsOptions.DEFAULTS.oTags);
            }

            _initializedTableTools = true;
        }
    }

    function deIntegrate() {
        if (_initializedTableTools && $.fn.DataTable.TableTools && _savedFn.TableTools) {
            $.extend(true, $.fn.DataTable.TableTools.classes, _savedFn.TableTools.classes);
            $.extend(true, $.fn.DataTable.TableTools.DEFAULTS.oTags, _savedFn.TableTools.oTags);
            _initializedTableTools = false;
        }
    }

    function _saveFnToBeOverrided() {
        if ($.fn.DataTable.TableTools) {
            _savedFn.TableTools = {
                classes: angular.copy($.fn.DataTable.TableTools.classes),
                oTags: angular.copy($.fn.DataTable.TableTools.DEFAULTS.oTags)
            };
        }
    }
}
dtBootstrapTableTools.$inject = ['DTPropertyUtil', 'DTBootstrapDefaultOptions'];


})(window, document, jQuery, angular);