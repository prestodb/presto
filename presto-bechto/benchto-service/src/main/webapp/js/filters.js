/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
(function () {
    'use strict';

    angular.module('benchmarkServiceUI.filters', [])
        .filter('duration', ['numberFilter', function (numberFilter) {
            return function (value) {
                if ((value / 1000) < 1) {
                    return numberFilter(value, 3, 4) + ' ms';
                }
                value /= 1000;

                if ((value / 60) < 1) {
                    return numberFilter(value, 3, 4) + ' s';
                }
                value /= 60;

                if ((value / 60) < 1) {
                    return numberFilter(value, 3, 4) + ' m';
                }
                value /= 60;

                return numberFilter(value, 3, 4) + ' h';
            }
        }])
        .filter('unit', ['numberFilter', 'durationFilter', function (numberFilter, durationFilter) {
            return function (value, unit) {
                var outputValueText = '';
                var outputUnitText = '';

                if (unit === 'MILLISECONDS') {
                    outputValueText = durationFilter(value)
                }
                else if (unit === 'BYTES') {
                    if ((value / 1000) < 1) {
                        return numberFilter(value, 3) + ' B';
                    }
                    value /= 1000;
                    if ((value / 1000) < 1) {
                        return numberFilter(value, 3) + ' kB';
                    }
                    value /= 1000;
                    if ((value / 1000) < 1) {
                        return numberFilter(value, 3) + ' MB';
                    }
                    value /= 1000;

                    if ((value / 1000) < 1) {
                        return numberFilter(value, 3) + ' GB';
                    }
                    value /= 1000;

                    return numberFilter(value, 3) + ' TB';
                }
                else if (unit === 'PERCENT') {
                    outputValueText += numberFilter(value, 2);
                    outputUnitText = '%';
                }
                else if (unit === 'QUERY_PER_SECOND') {
                    outputValueText += numberFilter(value, 2);
                    outputUnitText = 'query/sec';
                }
                else {
                    outputValueText += numberFilter(value, 2);
                    outputUnitText = unit;
                }

                return outputValueText + ' ' + outputUnitText;
            };
        }])
        .filter('capitalize', function () {
            return function (input) {
                if (input != null) {
                    input = input.toLowerCase();
                }
                return input.substring(0, 1).toUpperCase() + input.substring(1);
            }
        })
        .filter('benchmarkListFilter', function () {
            return function (benchmarkRuns, searchText) {
                if (!searchText) {
                    return benchmarkRuns;
                }
                var searchTokens = searchText.split(/\s+/);
                return _.filter(benchmarkRuns, function (benchmarkRun) {
                    var notMatch = _.find(searchTokens, function (searchToken) {
                        return benchmarkRun.uniqueName.indexOf(searchToken) < 0;
                    });
                    return !notMatch;
                });
            }
        });
}());
