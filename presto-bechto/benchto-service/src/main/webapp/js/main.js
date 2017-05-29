/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
(function () {
    'use strict';

    angular.module('benchmarkServiceUI', ['ngRoute', 'benchmarkServiceUI.controllers', 'benchmarkServiceUI.services', 'benchmarkServiceUI.filters',
                                          'ui.bootstrap'])
        .config(['$routeProvider', function ($routeProvider) {
            $routeProvider
                .when('/', {
                    templateUrl: 'partials/environmentList.html',
                    controller: 'EnvironmentListCtrl'
                })
                .when('/:environmentName', {
                    templateUrl: 'partials/benchmarkList.html',
                    controller: 'BenchmarkListCtrl',
                    reloadOnSearch: false
                })
                .when('/benchmark/:uniqueName', {
                    templateUrl: 'partials/benchmark.html',
                    controller: 'BenchmarkCtrl'
                })
                .when('/benchmark/:uniqueName/:benchmarkSequenceId', {
                    templateUrl: 'partials/benchmarkRun.html',
                    controller: 'BenchmarkRunCtrl'
                })
                .when('/environment/:environmentName', {
                    templateUrl: 'partials/environment.html',
                    controller: 'EnvironmentCtrl'
                })
                .when('/compare/:benchmarkNames/:benchmarkSequenceIds', {
                    templateUrl: 'partials/compare.html',
                    controller: 'CompareCtrl'
                })
                .otherwise({
                    templateUrl: 'partials/404.html'
                });
        }]);
}());
