window.React["default"] = window.React;
window.ReactDOM["default"] = window.ReactDOM;
(function (global, factory) {
    if (typeof define === "function" && define.amd) {
        define(["exports"], factory);
    } else if (typeof exports !== "undefined") {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.filter_props_from = mod.exports;
    }
})(this, function (exports) {
    "use strict";

    exports.filterPropsFrom = filterPropsFrom;
    var internalProps = {
        column: true,
        columns: true,
        sortable: true,
        filterable: true,
        sortBy: true,
        defaultSort: true,
        itemsPerPage: true,
        childNode: true,
        data: true,
        children: true
    };

    function filterPropsFrom(baseProps) {
        baseProps = baseProps || {};
        var props = {};
        for (var key in baseProps) {
            if (!(key in internalProps)) {
                props[key] = baseProps[key];
            }
        }

        return props;
    }
});

(function (global, factory) {
    if (typeof define === "function" && define.amd) {
        define(["exports"], factory);
    } else if (typeof exports !== "undefined") {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.to_array = mod.exports;
    }
})(this, function (exports) {
    "use strict";

    exports.toArray = toArray;

    function toArray(obj) {
        var ret = [];
        for (var attr in obj) {
            ret[attr] = obj;
        }

        return ret;
    }
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.stringable = mod.exports;
    }
})(this, function (exports) {
    'use strict';

    exports.stringable = stringable;

    function stringable(thing) {
        return thing !== null && typeof thing !== 'undefined' && typeof (thing.toString === 'function');
    }
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', './stringable'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('./stringable'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.stringable);
        global.extract_data_from = mod.exports;
    }
})(this, function (exports, _stringable) {
    'use strict';

    exports.extractDataFrom = extractDataFrom;

    function extractDataFrom(key, column) {
        var value;
        if (typeof key !== 'undefined' && key !== null && key.__reactableMeta === true) {
            value = key.data[column];
        } else {
            value = key[column];
        }

        if (typeof value !== 'undefined' && value !== null && value.__reactableMeta === true) {
            value = typeof value.props.value !== 'undefined' && value.props.value !== null ? value.props.value : value.value;
        }

        return (0, _stringable.stringable)(value) ? value : '';
    }
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.is_react_component = mod.exports;
    }
})(this, function (exports) {
    // this is a bit hacky - it'd be nice if React exposed an API for this
    'use strict';

    exports.isReactComponent = isReactComponent;

    function isReactComponent(thing) {
        return thing !== null && typeof thing === 'object' && typeof thing.props !== 'undefined';
    }
});

(function (global, factory) {
    if (typeof define === "function" && define.amd) {
        define(["exports"], factory);
    } else if (typeof exports !== "undefined") {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.unsafe = mod.exports;
    }
})(this, function (exports) {
    "use strict";

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    exports.unsafe = unsafe;
    exports.isUnsafe = isUnsafe;

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

    var Unsafe = (function () {
        function Unsafe(content) {
            _classCallCheck(this, Unsafe);

            this.content = content;
        }

        _createClass(Unsafe, [{
            key: "toString",
            value: function toString() {
                return this.content;
            }
        }]);

        return Unsafe;
    })();

    function unsafe(str) {
        return new Unsafe(str);
    }

    ;

    function isUnsafe(obj) {
        return obj instanceof Unsafe;
    }

    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', 'react-dom'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('react-dom'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.ReactDOM);
        global.filterer = mod.exports;
    }
})(this, function (exports, _react, _reactDom) {
    'use strict';

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var FiltererInput = (function (_React$Component) {
        _inherits(FiltererInput, _React$Component);

        function FiltererInput() {
            _classCallCheck(this, FiltererInput);

            _get(Object.getPrototypeOf(FiltererInput.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(FiltererInput, [{
            key: 'onChange',
            value: function onChange() {
                this.props.onFilter(_reactDom['default'].findDOMNode(this).value);
            }
        }, {
            key: 'render',
            value: function render() {
                return _react['default'].createElement('input', { type: 'text',
                    className: 'reactable-filter-input',
                    placeholder: this.props.placeholder,
                    value: this.props.value,
                    onKeyUp: this.onChange.bind(this),
                    onChange: this.onChange.bind(this) });
            }
        }]);

        return FiltererInput;
    })(_react['default'].Component);

    exports.FiltererInput = FiltererInput;
    ;

    var Filterer = (function (_React$Component2) {
        _inherits(Filterer, _React$Component2);

        function Filterer() {
            _classCallCheck(this, Filterer);

            _get(Object.getPrototypeOf(Filterer.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Filterer, [{
            key: 'render',
            value: function render() {
                if (typeof this.props.colSpan === 'undefined') {
                    throw new TypeError('Must pass a colSpan argument to Filterer');
                }

                return _react['default'].createElement(
                    'tr',
                    { className: 'reactable-filterer' },
                    _react['default'].createElement(
                        'td',
                        { colSpan: this.props.colSpan },
                        _react['default'].createElement(FiltererInput, { onFilter: this.props.onFilter,
                            value: this.props.value,
                            placeholder: this.props.placeholder })
                    )
                );
            }
        }]);

        return Filterer;
    })(_react['default'].Component);

    exports.Filterer = Filterer;
    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports);
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports);
        global.sort = mod.exports;
    }
})(this, function (exports) {
    'use strict';

    var Sort = {
        Numeric: function Numeric(a, b) {
            var valA = parseFloat(a.toString().replace(/,/g, ''));
            var valB = parseFloat(b.toString().replace(/,/g, ''));

            // Sort non-numeric values alphabetically at the bottom of the list
            if (isNaN(valA) && isNaN(valB)) {
                valA = a;
                valB = b;
            } else {
                if (isNaN(valA)) {
                    return 1;
                }
                if (isNaN(valB)) {
                    return -1;
                }
            }

            if (valA < valB) {
                return -1;
            }
            if (valA > valB) {
                return 1;
            }

            return 0;
        },

        NumericInteger: function NumericInteger(a, b) {
            if (isNaN(a) || isNaN(b)) {
                return a > b ? 1 : -1;
            }

            return a - b;
        },

        Currency: function Currency(a, b) {
            // Parse out dollar signs, then do a regular numeric sort
            a = a.replace(/[^0-9\.\-\,]+/g, '');
            b = b.replace(/[^0-9\.\-\,]+/g, '');

            return exports.Sort.Numeric(a, b);
        },

        Date: (function (_Date) {
            function Date(_x, _x2) {
                return _Date.apply(this, arguments);
            }

            Date.toString = function () {
                return _Date.toString();
            };

            return Date;
        })(function (a, b) {
            // Note: this function tries to do a standard javascript string -> date conversion
            // If you need more control over the date string format, consider using a different
            // date library and writing your own function
            var valA = Date.parse(a);
            var valB = Date.parse(b);

            // Handle non-date values with numeric sort
            // Sort non-numeric values alphabetically at the bottom of the list
            if (isNaN(valA) || isNaN(valB)) {
                return exports.Sort.Numeric(a, b);
            }

            if (valA > valB) {
                return 1;
            }
            if (valB > valA) {
                return -1;
            }

            return 0;
        }),

        CaseInsensitive: function CaseInsensitive(a, b) {
            return a.toLowerCase().localeCompare(b.toLowerCase());
        }
    };
    exports.Sort = Sort;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './lib/is_react_component', './lib/stringable', './unsafe'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./lib/is_react_component'), require('./lib/stringable'), require('./unsafe'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.is_react_component, global.stringable, global.unsafe);
        global.td = mod.exports;
    }
})(this, function (exports, _react, _libIs_react_component, _libStringable, _unsafe) {
    'use strict';

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Td = (function (_React$Component) {
        _inherits(Td, _React$Component);

        function Td() {
            _classCallCheck(this, Td);

            _get(Object.getPrototypeOf(Td.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Td, [{
            key: 'handleClick',
            value: function handleClick(e) {
                if (typeof this.props.handleClick === 'function') {
                    return this.props.handleClick(e, this);
                }
            }
        }, {
            key: 'render',
            value: function render() {
                var tdProps = {
                    className: this.props.className,
                    onClick: this.handleClick.bind(this)
                };

                if (typeof this.props.style !== 'undefined') {
                    tdProps.style = this.props.style;
                }

                // Attach any properties on the column to this Td object to allow things like custom event handlers
                if (typeof this.props.column === 'object') {
                    for (var key in this.props.column) {
                        if (key !== 'key' && key !== 'name') {
                            tdProps[key] = this.props.column[key];
                        }
                    }
                }

                var data = this.props.data;

                if (typeof this.props.children !== 'undefined') {
                    if ((0, _libIs_react_component.isReactComponent)(this.props.children)) {
                        data = this.props.children;
                    } else if (typeof this.props.data === 'undefined' && (0, _libStringable.stringable)(this.props.children)) {
                        data = this.props.children.toString();
                    }

                    if ((0, _unsafe.isUnsafe)(this.props.children)) {
                        tdProps.dangerouslySetInnerHTML = { __html: this.props.children.toString() };
                    } else {
                        tdProps.children = data;
                    }
                }

                return _react['default'].createElement('td', tdProps);
            }
        }]);

        return Td;
    })(_react['default'].Component);

    exports.Td = Td;
    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './td', './lib/to_array', './lib/filter_props_from'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./td'), require('./lib/to_array'), require('./lib/filter_props_from'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.td, global.to_array, global.filter_props_from);
        global.tr = mod.exports;
    }
})(this, function (exports, _react, _td, _libTo_array, _libFilter_props_from) {
    'use strict';

    var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Tr = (function (_React$Component) {
        _inherits(Tr, _React$Component);

        function Tr() {
            _classCallCheck(this, Tr);

            _get(Object.getPrototypeOf(Tr.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Tr, [{
            key: 'render',
            value: function render() {
                var children = (0, _libTo_array.toArray)(_react['default'].Children.children(this.props.children));

                if (this.props.data && this.props.columns && typeof this.props.columns.map === 'function') {
                    if (typeof children.concat === 'undefined') {
                        console.log(children);
                    }

                    children = children.concat(this.props.columns.map((function (column, i) {
                        if (this.props.data.hasOwnProperty(column.key)) {
                            var value = this.props.data[column.key];
                            var props = {};

                            if (typeof value !== 'undefined' && value !== null && value.__reactableMeta === true) {
                                props = value.props;
                                value = value.value;
                            }

                            return _react['default'].createElement(
                                _td.Td,
                                _extends({ column: column, key: column.key }, props),
                                value
                            );
                        } else {
                            return _react['default'].createElement(_td.Td, { column: column, key: column.key });
                        }
                    }).bind(this)));
                }

                // Manually transfer props
                var props = (0, _libFilter_props_from.filterPropsFrom)(this.props);

                return _react['default'].DOM.tr(props, children);
            }
        }]);

        return Tr;
    })(_react['default'].Component);

    exports.Tr = Tr;
    ;

    Tr.childNode = _td.Td;
    Tr.dataType = 'object';
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './unsafe', './lib/filter_props_from'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./unsafe'), require('./lib/filter_props_from'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.unsafe, global.filter_props_from);
        global.th = mod.exports;
    }
})(this, function (exports, _react, _unsafe, _libFilter_props_from) {
    'use strict';

    var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Th = (function (_React$Component) {
        _inherits(Th, _React$Component);

        function Th() {
            _classCallCheck(this, Th);

            _get(Object.getPrototypeOf(Th.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Th, [{
            key: 'render',
            value: function render() {
                var childProps = undefined;

                if ((0, _unsafe.isUnsafe)(this.props.children)) {
                    return _react['default'].createElement('th', _extends({}, (0, _libFilter_props_from.filterPropsFrom)(this.props), {
                        dangerouslySetInnerHTML: { __html: this.props.children.toString() } }));
                } else {
                    return _react['default'].createElement(
                        'th',
                        (0, _libFilter_props_from.filterPropsFrom)(this.props),
                        this.props.children
                    );
                }
            }
        }]);

        return Th;
    })(_react['default'].Component);

    exports.Th = Th;
    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './th', './filterer', './lib/filter_props_from'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./th'), require('./filterer'), require('./lib/filter_props_from'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.th, global.filterer, global.filter_props_from);
        global.thead = mod.exports;
    }
})(this, function (exports, _react, _th, _filterer, _libFilter_props_from) {
    'use strict';

    var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Thead = (function (_React$Component) {
        _inherits(Thead, _React$Component);

        function Thead() {
            _classCallCheck(this, Thead);

            _get(Object.getPrototypeOf(Thead.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Thead, [{
            key: 'handleClickTh',
            value: function handleClickTh(column) {
                this.props.onSort(column.key);
            }
        }, {
            key: 'handleKeyDownTh',
            value: function handleKeyDownTh(column, event) {
                if (event.keyCode === 13) {
                    this.props.onSort(column.key);
                }
            }
        }, {
            key: 'render',
            value: function render() {
                // Declare the list of Ths
                var Ths = [];
                for (var index = 0; index < this.props.columns.length; index++) {
                    var column = this.props.columns[index];
                    var thClass = 'reactable-th-' + column.key.replace(/\s+/g, '-').toLowerCase();
                    var sortClass = '';

                    if (this.props.sortableColumns[column.key]) {
                        sortClass += 'reactable-header-sortable ';
                    }

                    if (this.props.sort.column === column.key) {
                        sortClass += 'reactable-header-sort';
                        if (this.props.sort.direction === 1) {
                            sortClass += '-asc';
                        } else {
                            sortClass += '-desc';
                        }
                    }

                    if (sortClass.length > 0) {
                        thClass += ' ' + sortClass;
                    }

                    if (typeof column.props === 'object' && typeof column.props.className === 'string') {
                        thClass += ' ' + column.props.className;
                    }

                    Ths.push(_react['default'].createElement(
                        _th.Th,
                        _extends({}, column.props, {
                            className: thClass,
                            key: index,
                            onClick: this.handleClickTh.bind(this, column),
                            onKeyDown: this.handleKeyDownTh.bind(this, column),
                            role: 'button',
                            tabIndex: '0' }),
                        column.label
                    ));
                }

                // Manually transfer props
                var props = (0, _libFilter_props_from.filterPropsFrom)(this.props);

                return _react['default'].createElement(
                    'thead',
                    props,
                    this.props.filtering === true ? _react['default'].createElement(_filterer.Filterer, {
                        colSpan: this.props.columns.length,
                        onFilter: this.props.onFilter,
                        placeholder: this.props.filterPlaceholder,
                        value: this.props.currentFilter
                    }) : null,
                    _react['default'].createElement(
                        'tr',
                        { className: 'reactable-column-header' },
                        Ths
                    )
                );
            }
        }], [{
            key: 'getColumns',
            value: function getColumns(component) {
                // Can't use React.Children.map since that doesn't return a proper array
                var columns = [];
                _react['default'].Children.forEach(component.props.children, function (th) {
                    var column = {};
                    if (typeof th.props !== 'undefined') {
                        column.props = (0, _libFilter_props_from.filterPropsFrom)(th.props);

                        // use the content as the label & key
                        if (typeof th.props.children !== 'undefined') {
                            column.label = th.props.children;
                            column.key = column.label;
                        }

                        // the key in the column attribute supersedes the one defined previously
                        if (typeof th.props.column === 'string') {
                            column.key = th.props.column;

                            // in case we don't have a label yet
                            if (typeof column.label === 'undefined') {
                                column.label = column.key;
                            }
                        }
                    }

                    if (typeof column.key === 'undefined') {
                        throw new TypeError('<th> must have either a "column" property or a string ' + 'child');
                    } else {
                        columns.push(column);
                    }
                });

                return columns;
            }
        }]);

        return Thead;
    })(_react['default'].Component);

    exports.Thead = Thead;
    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React);
        global.tfoot = mod.exports;
    }
})(this, function (exports, _react) {
    'use strict';

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Tfoot = (function (_React$Component) {
        _inherits(Tfoot, _React$Component);

        function Tfoot() {
            _classCallCheck(this, Tfoot);

            _get(Object.getPrototypeOf(Tfoot.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Tfoot, [{
            key: 'render',
            value: function render() {
                return _react['default'].createElement('tfoot', this.props);
            }
        }]);

        return Tfoot;
    })(_react['default'].Component);

    exports.Tfoot = Tfoot;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React);
        global.paginator = mod.exports;
    }
})(this, function (exports, _react) {
    'use strict';

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    function pageHref(num) {
        return '#page-' + (num + 1);
    }

    var Paginator = (function (_React$Component) {
        _inherits(Paginator, _React$Component);

        function Paginator() {
            _classCallCheck(this, Paginator);

            _get(Object.getPrototypeOf(Paginator.prototype), 'constructor', this).apply(this, arguments);
        }

        _createClass(Paginator, [{
            key: 'handlePrevious',
            value: function handlePrevious(e) {
                e.preventDefault();
                this.props.onPageChange(this.props.currentPage - 1);
            }
        }, {
            key: 'handleNext',
            value: function handleNext(e) {
                e.preventDefault();
                this.props.onPageChange(this.props.currentPage + 1);
            }
        }, {
            key: 'handlePageButton',
            value: function handlePageButton(page, e) {
                e.preventDefault();
                this.props.onPageChange(page);
            }
        }, {
            key: 'renderPrevious',
            value: function renderPrevious() {
                if (this.props.currentPage > 0) {
                    return _react['default'].createElement(
                        'a',
                        { className: 'reactable-previous-page',
                            href: pageHref(this.props.currentPage - 1),
                            onClick: this.handlePrevious.bind(this) },
                        this.props.previousPageLabel || 'Previous'
                    );
                }
            }
        }, {
            key: 'renderNext',
            value: function renderNext() {
                if (this.props.currentPage < this.props.numPages - 1) {
                    return _react['default'].createElement(
                        'a',
                        { className: 'reactable-next-page',
                            href: pageHref(this.props.currentPage + 1),
                            onClick: this.handleNext.bind(this) },
                        this.props.nextPageLabel || 'Next'
                    );
                }
            }
        }, {
            key: 'renderPageButton',
            value: function renderPageButton(className, pageNum) {

                return _react['default'].createElement(
                    'a',
                    { className: className,
                        key: pageNum,
                        href: pageHref(pageNum),
                        onClick: this.handlePageButton.bind(this, pageNum) },
                    pageNum + 1
                );
            }
        }, {
            key: 'render',
            value: function render() {
                if (typeof this.props.colSpan === 'undefined') {
                    throw new TypeError('Must pass a colSpan argument to Paginator');
                }

                if (typeof this.props.numPages === 'undefined') {
                    throw new TypeError('Must pass a non-zero numPages argument to Paginator');
                }

                if (typeof this.props.currentPage === 'undefined') {
                    throw new TypeError('Must pass a currentPage argument to Paginator');
                }

                var pageButtons = [];
                var pageButtonLimit = this.props.pageButtonLimit;
                var currentPage = this.props.currentPage;
                var numPages = this.props.numPages;
                var lowerHalf = Math.round(pageButtonLimit / 2);
                var upperHalf = pageButtonLimit - lowerHalf;

                for (var i = 0; i < this.props.numPages; i++) {
                    var showPageButton = false;
                    var pageNum = i;
                    var className = "reactable-page-button";
                    if (currentPage === i) {
                        className += " reactable-current-page";
                    }
                    pageButtons.push(this.renderPageButton(className, pageNum));
                }

                if (currentPage - pageButtonLimit + lowerHalf > 0) {
                    if (currentPage > numPages - lowerHalf) {
                        pageButtons.splice(0, numPages - pageButtonLimit);
                    } else {
                        pageButtons.splice(0, currentPage - pageButtonLimit + lowerHalf);
                    }
                }

                if (numPages - currentPage > upperHalf) {
                    pageButtons.splice(pageButtonLimit, pageButtons.length - pageButtonLimit);
                }

                return _react['default'].createElement(
                    'tbody',
                    { className: 'reactable-pagination' },
                    _react['default'].createElement(
                        'tr',
                        null,
                        _react['default'].createElement(
                            'td',
                            { colSpan: this.props.colSpan },
                            this.renderPrevious(),
                            pageButtons,
                            this.renderNext()
                        )
                    )
                );
            }
        }]);

        return Paginator;
    })(_react['default'].Component);

    exports.Paginator = Paginator;
    ;
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './lib/filter_props_from', './lib/extract_data_from', './unsafe', './thead', './th', './tr', './tfoot', './paginator'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./lib/filter_props_from'), require('./lib/extract_data_from'), require('./unsafe'), require('./thead'), require('./th'), require('./tr'), require('./tfoot'), require('./paginator'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.filter_props_from, global.extract_data_from, global.unsafe, global.thead, global.th, global.tr, global.tfoot, global.paginator);
        global.table = mod.exports;
    }
})(this, function (exports, _react, _libFilter_props_from, _libExtract_data_from, _unsafe, _thead, _th, _tr, _tfoot, _paginator) {
    'use strict';

    var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

    var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

    var _get = function get(_x, _x2, _x3) { var _again = true; _function: while (_again) { var object = _x, property = _x2, receiver = _x3; _again = false; if (object === null) object = Function.prototype; var desc = Object.getOwnPropertyDescriptor(object, property); if (desc === undefined) { var parent = Object.getPrototypeOf(object); if (parent === null) { return undefined; } else { _x = parent; _x2 = property; _x3 = receiver; _again = true; desc = parent = undefined; continue _function; } } else if ('value' in desc) { return desc.value; } else { var getter = desc.get; if (getter === undefined) { return undefined; } return getter.call(receiver); } } };

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } }

    function _inherits(subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

    var Table = (function (_React$Component) {
        _inherits(Table, _React$Component);

        function Table(props) {
            _classCallCheck(this, Table);

            _get(Object.getPrototypeOf(Table.prototype), 'constructor', this).call(this, props);

            this.state = {
                currentPage: 0,
                currentSort: {
                    column: null,
                    direction: this.props.defaultSortDescending ? -1 : 1
                },
                filter: ''
            };

            // Set the state of the current sort to the default sort
            if (props.sortBy !== false || props.defaultSort !== false) {
                var sortingColumn = props.sortBy || props.defaultSort;
                this.state.currentSort = this.getCurrentSort(sortingColumn);
            }
        }

        _createClass(Table, [{
            key: 'filterBy',
            value: function filterBy(filter) {
                this.setState({ filter: filter });
            }

            // Translate a user defined column array to hold column objects if strings are specified
            // (e.g. ['column1'] => [{key: 'column1', label: 'column1'}])
        }, {
            key: 'translateColumnsArray',
            value: function translateColumnsArray(columns) {
                return columns.map((function (column, i) {
                    if (typeof column === 'string') {
                        return {
                            key: column,
                            label: column
                        };
                    } else {
                        if (typeof column.sortable !== 'undefined') {
                            var sortFunction = column.sortable === true ? 'default' : column.sortable;
                            this._sortable[column.key] = sortFunction;
                        }

                        return column;
                    }
                }).bind(this));
            }
        }, {
            key: 'parseChildData',
            value: function parseChildData(props) {
                var data = [],
                    tfoot = undefined;

                // Transform any children back to a data array
                if (typeof props.children !== 'undefined') {
                    _react['default'].Children.forEach(props.children, (function (child) {
                        if (typeof child === 'undefined' || child === null) {
                            return;
                        }

                        switch (child.type) {
                            case _thead.Thead:
                                break;
                            case _tfoot.Tfoot:
                                if (typeof tfoot !== 'undefined') {
                                    console.warn('You can only have one <Tfoot>, but more than one was specified.' + 'Ignoring all but the last one');
                                }
                                tfoot = child;
                                break;
                            case _tr.Tr:
                                var childData = child.props.data || {};

                                _react['default'].Children.forEach(child.props.children, function (descendant) {
                                    // TODO
                                    /* if (descendant.type.ConvenienceConstructor === Td) { */
                                    if (typeof descendant !== 'object' || descendant == null) {
                                        return;
                                    } else if (typeof descendant.props.column !== 'undefined') {
                                        var value = undefined;

                                        if (typeof descendant.props.data !== 'undefined') {
                                            value = descendant.props.data;
                                        } else if (typeof descendant.props.children !== 'undefined') {
                                            value = descendant.props.children;
                                        } else {
                                            console.warn('exports.Td specified without ' + 'a `data` property or children, ' + 'ignoring');
                                            return;
                                        }

                                        childData[descendant.props.column] = {
                                            value: value,
                                            props: (0, _libFilter_props_from.filterPropsFrom)(descendant.props),
                                            __reactableMeta: true
                                        };
                                    } else {
                                        console.warn('exports.Td specified without a ' + '`column` property, ignoring');
                                    }
                                });

                                data.push({
                                    data: childData,
                                    props: (0, _libFilter_props_from.filterPropsFrom)(child.props),
                                    __reactableMeta: true
                                });
                                break;

                            default:
                                console.warn('The only possible children of <Table> are <Thead>, <Tr>, ' + 'or one <Tfoot>.');
                        }
                    }).bind(this));
                }

                return { data: data, tfoot: tfoot };
            }
        }, {
            key: 'initialize',
            value: function initialize(props) {
                this.data = props.data || [];

                var _parseChildData = this.parseChildData(props);

                var data = _parseChildData.data;
                var tfoot = _parseChildData.tfoot;

                this.data = this.data.concat(data);
                this.tfoot = tfoot;

                this.initializeSorts(props);
                this.initializeFilters(props);
            }
        }, {
            key: 'initializeFilters',
            value: function initializeFilters() {
                this._filterable = {};
                // Transform filterable properties into a more friendly list
                for (var i in this.props.filterable) {
                    var column = this.props.filterable[i];
                    var columnName = undefined,
                        filterFunction = undefined;

                    if (column instanceof Object) {
                        if (typeof column.column !== 'undefined') {
                            columnName = column.column;
                        } else {
                            console.warn('Filterable column specified without column name');
                            continue;
                        }

                        if (typeof column.filterFunction === 'function') {
                            filterFunction = column.filterFunction;
                        } else {
                            filterFunction = 'default';
                        }
                    } else {
                        columnName = column;
                        filterFunction = 'default';
                    }

                    this._filterable[columnName] = filterFunction;
                }
            }
        }, {
            key: 'initializeSorts',
            value: function initializeSorts() {
                this._sortable = {};
                // Transform sortable properties into a more friendly list
                for (var i in this.props.sortable) {
                    var column = this.props.sortable[i];
                    var columnName = undefined,
                        sortFunction = undefined;

                    if (column instanceof Object) {
                        if (typeof column.column !== 'undefined') {
                            columnName = column.column;
                        } else {
                            console.warn('Sortable column specified without column name');
                            return;
                        }

                        if (typeof column.sortFunction === 'function') {
                            sortFunction = column.sortFunction;
                        } else {
                            sortFunction = 'default';
                        }
                    } else {
                        columnName = column;
                        sortFunction = 'default';
                    }

                    this._sortable[columnName] = sortFunction;
                }
            }
        }, {
            key: 'getCurrentSort',
            value: function getCurrentSort(column) {
                var columnName = undefined,
                    sortDirection = undefined;

                if (column instanceof Object) {
                    if (typeof column.column !== 'undefined') {
                        columnName = column.column;
                    } else {
                        console.warn('Default column specified without column name');
                        return;
                    }

                    if (typeof column.direction !== 'undefined') {
                        if (column.direction === 1 || column.direction === 'asc') {
                            sortDirection = 1;
                        } else if (column.direction === -1 || column.direction === 'desc') {
                            sortDirection = -1;
                        } else {
                            var defaultDirection = this.props.defaultSortDescending ? 'descending' : 'ascending';

                            console.warn('Invalid default sort specified. Defaulting to ' + defaultDirection);
                            sortDirection = this.props.defaultSortDescending ? -1 : 1;
                        }
                    } else {
                        sortDirection = this.props.defaultSortDescending ? -1 : 1;
                    }
                } else {
                    columnName = column;
                    sortDirection = this.props.defaultSortDescending ? -1 : 1;
                }

                return {
                    column: columnName,
                    direction: sortDirection
                };
            }
        }, {
            key: 'updateCurrentSort',
            value: function updateCurrentSort(sortBy) {
                if (sortBy !== false && sortBy.column !== this.state.currentSort.column && sortBy.direction !== this.state.currentSort.direction) {

                    this.setState({ currentSort: this.getCurrentSort(sortBy) });
                }
            }
        }, {
            key: 'componentWillMount',
            value: function componentWillMount() {
                this.initialize(this.props);
                this.sortByCurrentSort();
                this.filterBy(this.props.filterBy);
            }
        }, {
            key: 'componentWillReceiveProps',
            value: function componentWillReceiveProps(nextProps) {
                this.initialize(nextProps);
                this.updateCurrentSort(nextProps.sortBy);
                this.sortByCurrentSort();
                this.filterBy(nextProps.filterBy);
            }
        }, {
            key: 'applyFilter',
            value: function applyFilter(filter, children) {
                // Helper function to apply filter text to a list of table rows
                filter = filter.toLowerCase();
                var matchedChildren = [];

                for (var i = 0; i < children.length; i++) {
                    var data = children[i].props.data;

                    for (var filterColumn in this._filterable) {
                        if (typeof data[filterColumn] !== 'undefined') {
                            // Default filter
                            if (typeof this._filterable[filterColumn] === 'undefined' || this._filterable[filterColumn] === 'default') {
                                if ((0, _libExtract_data_from.extractDataFrom)(data, filterColumn).toString().toLowerCase().indexOf(filter) > -1) {
                                    matchedChildren.push(children[i]);
                                    break;
                                }
                            } else {
                                // Apply custom filter
                                if (this._filterable[filterColumn]((0, _libExtract_data_from.extractDataFrom)(data, filterColumn).toString(), filter)) {
                                    matchedChildren.push(children[i]);
                                    break;
                                }
                            }
                        }
                    }
                }

                return matchedChildren;
            }
        }, {
            key: 'sortByCurrentSort',
            value: function sortByCurrentSort() {
                // Apply a sort function according to the current sort in the state.
                // This allows us to perform a default sort even on a non sortable column.
                var currentSort = this.state.currentSort;

                if (currentSort.column === null) {
                    return;
                }

                this.data.sort((function (a, b) {
                    var keyA = (0, _libExtract_data_from.extractDataFrom)(a, currentSort.column);
                    keyA = (0, _unsafe.isUnsafe)(keyA) ? keyA.toString() : keyA || '';
                    var keyB = (0, _libExtract_data_from.extractDataFrom)(b, currentSort.column);
                    keyB = (0, _unsafe.isUnsafe)(keyB) ? keyB.toString() : keyB || '';

                    // Default sort
                    if (typeof this._sortable[currentSort.column] === 'undefined' || this._sortable[currentSort.column] === 'default') {

                        // Reverse direction if we're doing a reverse sort
                        if (keyA < keyB) {
                            return -1 * currentSort.direction;
                        }

                        if (keyA > keyB) {
                            return 1 * currentSort.direction;
                        }

                        return 0;
                    } else {
                        // Reverse columns if we're doing a reverse sort
                        if (currentSort.direction === 1) {
                            return this._sortable[currentSort.column](keyA, keyB);
                        } else {
                            return this._sortable[currentSort.column](keyB, keyA);
                        }
                    }
                }).bind(this));
            }
        }, {
            key: 'onSort',
            value: function onSort(column) {
                // Don't perform sort on unsortable columns
                if (typeof this._sortable[column] === 'undefined') {
                    return;
                }

                var currentSort = this.state.currentSort;

                if (currentSort.column === column) {
                    currentSort.direction *= -1;
                } else {
                    currentSort.column = column;
                    currentSort.direction = this.props.defaultSortDescending ? -1 : 1;
                }

                // Set the current sort and pass it to the sort function
                this.setState({ currentSort: currentSort });
                this.sortByCurrentSort();

                if (typeof this.props.onSort === 'function') {
                    this.props.onSort(currentSort);
                }
            }
        }, {
            key: 'render',
            value: function render() {
                var _this = this;

                var children = [];
                var columns = undefined;
                var userColumnsSpecified = false;

                var firstChild = null;

                if (this.props.children && this.props.children.length > 0 && this.props.children[0].type === _thead.Thead) {
                    firstChild = this.props.children[0];
                } else if (typeof this.props.children !== 'undefined' && this.props.children.type === _thead.Thead) {
                    firstChild = this.props.children;
                }

                if (firstChild !== null) {
                    columns = _thead.Thead.getColumns(firstChild);
                } else {
                    columns = this.props.columns || [];
                }

                if (columns.length > 0) {
                    userColumnsSpecified = true;
                    columns = this.translateColumnsArray(columns);
                }

                // Build up table rows
                if (this.data && typeof this.data.map === 'function') {
                    // Build up the columns array
                    children = children.concat(this.data.map((function (rawData, i) {
                        var data = rawData;
                        var props = {};
                        if (rawData.__reactableMeta === true) {
                            data = rawData.data;
                            props = rawData.props;
                        }

                        // Loop through the keys in each data row and build a td for it
                        for (var k in data) {
                            if (data.hasOwnProperty(k)) {
                                // Update the columns array with the data's keys if columns were not
                                // already specified
                                if (userColumnsSpecified === false) {
                                    (function () {
                                        var column = {
                                            key: k,
                                            label: k
                                        };

                                        // Only add a new column if it doesn't already exist in the columns array
                                        if (columns.find(function (element) {
                                            return element.key === column.key;
                                        }) === undefined) {
                                            columns.push(column);
                                        }
                                    })();
                                }
                            }
                        }

                        return _react['default'].createElement(_tr.Tr, _extends({ columns: columns, key: i, data: data }, props));
                    }).bind(this)));
                }

                if (this.props.sortable === true) {
                    for (var i = 0; i < columns.length; i++) {
                        this._sortable[columns[i].key] = 'default';
                    }
                }

                // Determine if we render the filter box
                var filtering = false;
                if (this.props.filterable && Array.isArray(this.props.filterable) && this.props.filterable.length > 0 && !this.props.hideFilterInput) {
                    filtering = true;
                }

                // Apply filters
                var filteredChildren = children;
                if (this.state.filter !== '') {
                    filteredChildren = this.applyFilter(this.state.filter, filteredChildren);
                }

                // Determine pagination properties and which columns to display
                var itemsPerPage = 0;
                var pagination = false;
                var numPages = undefined;
                var currentPage = this.state.currentPage;
                var pageButtonLimit = this.props.pageButtonLimit || 10;

                var currentChildren = filteredChildren;
                if (this.props.itemsPerPage > 0) {
                    itemsPerPage = this.props.itemsPerPage;
                    numPages = Math.ceil(filteredChildren.length / itemsPerPage);

                    if (currentPage > numPages - 1) {
                        currentPage = numPages - 1;
                    }

                    pagination = true;
                    currentChildren = filteredChildren.slice(currentPage * itemsPerPage, (currentPage + 1) * itemsPerPage);
                }

                // Manually transfer props
                var props = (0, _libFilter_props_from.filterPropsFrom)(this.props);

                var noDataText = this.props.noDataText ? _react['default'].createElement(
                    'tr',
                    { className: 'reactable-no-data' },
                    _react['default'].createElement(
                        'td',
                        { colSpan: columns.length },
                        this.props.noDataText
                    )
                ) : null;

                return _react['default'].createElement(
                    'table',
                    props,
                    columns && columns.length > 0 ? _react['default'].createElement(_thead.Thead, { columns: columns,
                        filtering: filtering,
                        onFilter: function (filter) {
                            _this.setState({ filter: filter });
                            if (_this.props.onFilter) {
                                _this.props.onFilter(filter);
                            }
                        },
                        filterPlaceholder: this.props.filterPlaceholder,
                        currentFilter: this.state.filter,
                        sort: this.state.currentSort,
                        sortableColumns: this._sortable,
                        onSort: this.onSort.bind(this),
                        key: 'thead' }) : null,
                    _react['default'].createElement(
                        'tbody',
                        { className: 'reactable-data', key: 'tbody' },
                        currentChildren.length > 0 ? currentChildren : noDataText
                    ),
                    pagination === true ? _react['default'].createElement(_paginator.Paginator, { colSpan: columns.length,
                        pageButtonLimit: pageButtonLimit,
                        numPages: numPages,
                        currentPage: currentPage,
                        onPageChange: function (page) {
                            _this.setState({ currentPage: page });
                            if (_this.props.onPageChange) {
                                _this.props.onPageChange(page);
                            }
                        },
                        previousPageLabel: this.props.previousPageLabel,
                        nextPageLabel: this.props.nextPageLabel,
                        key: 'paginator' }) : null,
                    this.tfoot
                );
            }
        }]);

        return Table;
    })(_react['default'].Component);

    exports.Table = Table;

    Table.defaultProps = {
        sortBy: false,
        defaultSort: false,
        defaultSortDescending: false,
        itemsPerPage: 0,
        filterBy: '',
        hideFilterInput: false
    };
});

(function (global, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['exports', 'react', './reactable/table', './reactable/tr', './reactable/td', './reactable/th', './reactable/tfoot', './reactable/thead', './reactable/sort', './reactable/unsafe'], factory);
    } else if (typeof exports !== 'undefined') {
        factory(exports, require('react'), require('./reactable/table'), require('./reactable/tr'), require('./reactable/td'), require('./reactable/th'), require('./reactable/tfoot'), require('./reactable/thead'), require('./reactable/sort'), require('./reactable/unsafe'));
    } else {
        var mod = {
            exports: {}
        };
        factory(mod.exports, global.React, global.table, global.tr, global.td, global.th, global.tfoot, global.thead, global.sort, global.unsafe);
        global.reactable = mod.exports;
    }
})(this, function (exports, _react, _reactableTable, _reactableTr, _reactableTd, _reactableTh, _reactableTfoot, _reactableThead, _reactableSort, _reactableUnsafe) {
    'use strict';

    _react['default'].Children.children = function (children) {
        return _react['default'].Children.map(children, function (x) {
            return x;
        }) || [];
    };

    // Array.prototype.find polyfill - see https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/find
    if (!Array.prototype.find) {
        Object.defineProperty(Array.prototype, 'find', {
            enumerable: false,
            configurable: true,
            writable: true,
            value: function value(predicate) {
                if (this === null) {
                    throw new TypeError('Array.prototype.find called on null or undefined');
                }
                if (typeof predicate !== 'function') {
                    throw new TypeError('predicate must be a function');
                }
                var list = Object(this);
                var length = list.length >>> 0;
                var thisArg = arguments[1];
                var value;
                for (var i = 0; i < length; i++) {
                    if (i in list) {
                        value = list[i];
                        if (predicate.call(thisArg, value, i, list)) {
                            return value;
                        }
                    }
                }
                return undefined;
            }
        });
    }

    var Reactable = { Table: _reactableTable.Table, Tr: _reactableTr.Tr, Td: _reactableTd.Td, Th: _reactableTh.Th, Tfoot: _reactableTfoot.Tfoot, Thead: _reactableThead.Thead, Sort: _reactableSort.Sort, unsafe: _reactableUnsafe.unsafe };

    exports['default'] = Reactable;

    if (typeof window !== 'undefined') {
        window.Reactable = Reactable;
    }
});
