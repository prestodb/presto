/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
(function () {
    'use strict';

    angular.module('benchmarkServiceUI.controllers', ['benchmarkServiceUI.services', 'nvd3', 'datatables', 'datatables.colvis', 'datatables.bootstrap'])
        .controller('BenchmarkListCtrl', ['$scope', '$routeParams', '$location', 'BenchmarkService', 'CartCompareService', 'DTOptionsBuilder', 'DTColumnDefBuilder',
            function ($scope, $routeParams, $location, BenchmarkService, CartCompareService, DTOptionsBuilder, DTColumnDefBuilder) {

                $scope.environmentName = $routeParams.environmentName

                BenchmarkService.loadLatestBenchmarkRuns($scope.environmentName)
                    .then(function (latestBenchmarkRuns) {
                        $scope.availableVariables = _.chain(latestBenchmarkRuns)
                            .map(function (benchmarkRun) { return _.keys(benchmarkRun.variables); })
                            .flatten()
                            .uniq()
                            .map(function (variableName) { return {name: variableName, visible: false}})
                            .value();

                        for (var i = 0; i < Math.min(8, $scope.availableVariables.length); ++i) {
                            $scope.availableVariables[i].visible = true;
                        }

                        var variableColumns = $scope.availableVariables.length;
                        $scope.dtOptions = DTOptionsBuilder.newOptions()
                            .withPaginationType('full_numbers')
                            .withBootstrap()
                            .withColVis()
                            // predefined visibility
                            .withColVisOption(
                                'aiExclude',
                                [0, 1, 2, 4 + variableColumns + 1]) //[cart, name, sequence id, mean duration]
                            // disable initial sorting
                            .withOption("aaSorting", [])
                            // pagination at the top too
                            .withOption("dom", '<"top"Clf<"clear">ip<"clear">>rt<"bottom"ip<"clear">>');

                        $scope.dtColumnDefs = _.chain($scope.availableVariables)
                            .map(function(availableVariable, index) {
                                var columnDef = DTColumnDefBuilder.newColumnDef(4 + index);
                                if (!availableVariable.visible) {
                                    columnDef.notVisible();
                                }
                                return columnDef;
                            })
                            .value().concat([
                                DTColumnDefBuilder.newColumnDef(0).withOption("width", "1em").notSortable(),
                                // unique name should be always hidden (used for search only)
                                DTColumnDefBuilder.newColumnDef(1).notVisible(),
                                DTColumnDefBuilder.newColumnDef(4 + variableColumns).notVisible() //started column
                            ]);

                        CartHelper.setCartAddedFlag(CartCompareService, latestBenchmarkRuns);

                        $scope.latestBenchmarkRuns = latestBenchmarkRuns;
                    });

                $scope.$on('cart:added', function (event, benchmarkRun) {
                    CartHelper.toggleCartAddedFlag(CartCompareService, $scope.latestBenchmarkRuns, benchmarkRun, true);
                });

                $scope.$on('cart:removed', function (event, benchmarkRun) {
                    CartHelper.toggleCartAddedFlag(CartCompareService, $scope.latestBenchmarkRuns, benchmarkRun, false);
                });

                $scope.addedToCompareChanged = function (benchmarkRun) {
                    CartHelper.updateBenchmarkCartSelection(CartCompareService, benchmarkRun);
                };

                $scope.dtInstanceCallback = function(dtInstance)
                {
                    var datatableObj = dtInstance.DataTable;
                    $scope.tableInstance = datatableObj;
                    $scope.tableInstance.on('search.dt', function () {
                        if($scope.$$phase) {
                            return;
                        }
                        $scope.$apply(function() {
                            $location.search('query', $scope.tableInstance.search()).replace();
                        });
                    });

                    var query = $location.search()['query'];
                    if (query) {
                        $scope.tableInstance.search(query).draw();
                    }
                };
            }])
        .controller('BenchmarkCtrl', ['$scope', '$routeParams', '$location', '$filter', 'BenchmarkService', 'CartCompareService', 'TagService',
            function ($scope, $routeParams, $location, $filter, BenchmarkService, CartCompareService, TagService) {
                $scope.uniqueName = $routeParams.uniqueName;
                $scope.environmentName = $routeParams.environment;

                $scope.onBenchmarkClick = function (points, evt) {
                    if (points) {
                        var benchmarkRunSequenceId = points[0].label;
                        $location.path('benchmark/' + $routeParams.uniqueName + '/' + benchmarkRunSequenceId);
                    }
                };

                $scope.$on('cart:added', function (event, benchmarkRun) {
                    CartHelper.toggleCartAddedFlag(CartCompareService, $scope.benchmarkRuns, benchmarkRun, true);
                });

                $scope.$on('cart:removed', function (event, benchmarkRun) {
                    CartHelper.toggleCartAddedFlag(CartCompareService, $scope.benchmarkRuns, benchmarkRun, false);
                });

                $scope.addedToCompareChanged = function (benchmarkRun) {
                    CartHelper.updateBenchmarkCartSelection(CartCompareService, benchmarkRun);
                };

                BenchmarkService.loadBenchmark($routeParams.uniqueName, $routeParams.environment)
                    .then(function (runs) {
                        $scope.benchmarkRuns = runs;

                        CartHelper.setCartAddedFlag(CartCompareService, runs);

                        // filter out benchmark runs which have not finished
                        var benchmarkRuns = _.filter(runs.slice().reverse(), function (benchmarkRun) {
                            return benchmarkRun.status === 'ENDED';
                        });

                        if (benchmarkRuns.length > 0) {
                            var start = benchmarkRuns[0].started;
                            var end = benchmarkRuns[benchmarkRuns.length - 1].stated;
                            TagService.loadTags($routeParams.environment, start, end)
                                .then(function(tags) {
                                    var benchmarkRunsHelper = new BenchmarkRunsHelper(benchmarkRuns, tags);
                                    $scope.aggregatedExecutionsMeasurementGraphsData = benchmarkRunsHelper.aggregatedExecutionsMeasurementGraphsData('lineChart', $filter, $location);
                                    $scope.benchmarkMeasurementGraphsData = benchmarkRunsHelper.benchmarkMeasurementGraphsData('lineChart', $filter, $location);

                                    for (var benchmarkRun in $scope.benchmarkRuns) {
                                        for (var tag in tags) {
                                            if ($scope.benchmarkRuns[benchmarkRun].started >= tags[tag].created) {
                                                $scope.benchmarkRuns[benchmarkRun].tag = tags[tag];
                                            } else if ($scope.benchmarkRuns[benchmarkRun].started < tags[tag].created) {
                                                break;
                                            }
                                        }
                                    }
                                })
                        }
                    });
            }])
        .controller('BenchmarkRunCtrl', ['$scope', '$routeParams', '$modal', 'BenchmarkService', 'CartCompareService', 'TagService',
            function ($scope, $routeParams, $modal, BenchmarkService, CartCompareService, TagService) {
                BenchmarkService.loadBenchmarkRun($routeParams.uniqueName, $routeParams.benchmarkSequenceId)
                    .then(function (benchmarkRun) {
                        $scope.benchmarkRun = benchmarkRun;

                        TagService.loadLatest(benchmarkRun.environment.name, benchmarkRun.started)
                            .then(function(tag) {
                                $scope.tag = tag;
                            })
                    });

                $scope.benchmarkFromParam = function (benchmarkRun) {
                    return benchmarkRun.started - 10 * 1000; // 10 seconds before start
                };

                $scope.benchmarkToParam = function (benchmarkRun) {
                    if (benchmarkRun.ended) {
                        return benchmarkRun.ended + 10 * 1000; // 10 seconds after end
                    }
                    return Date.now();
                };

                $scope.measurementUnit = function (measurementKey) {
                    return $scope.benchmarkRun.aggregatedMeasurements[measurementKey].unit;
                };

                $scope.showFailure = function (execution) {
                    $modal.open({
                        templateUrl: 'partials/benchmarkRunErrorModal.html',
                        controller: 'BenchmarkRunErrorCtrl',
                        size: 'lg',
                        resolve: {
                            failure: function () {
                                return {
                                    executionName: execution.name,
                                    message: execution.attributes.failureMessage,
                                    stackTrace: execution.attributes.failureStackTrace,
                                    SQLErrorCode: execution.attributes.failureSQLErrorCode
                                };
                            }
                        }
                    });
                };

                $scope.addToCompare = function (benchmarkRun) {
                    CartCompareService.add(benchmarkRun);
                };

                $scope.isAddedToCompare = function (benchmarkRun) {
                    return CartCompareService.contains(benchmarkRun);
                }
            }])
        .controller('EnvironmentListCtrl', ['$scope', 'EnvironmentService',
            function ($scope, EnvironmentService) {
                EnvironmentService.loadEnvironments()
                    .then(function (environments) {
                        $scope.environments = environments;
                    });
            }])
        .controller('EnvironmentCtrl', ['$scope', '$routeParams', 'EnvironmentService', 'TagService', function ($scope, $routeParams, EnvironmentService, TagService) {
            EnvironmentService.loadEnvironment($routeParams.environmentName)
                .then(function (environment) {
                    $scope.environment = environment;
                });
            TagService.loadTags($routeParams.environmentName)
                .then(function (tags) {
                    $scope.tags = tags;
                });
        }])
        .controller('BenchmarkRunErrorCtrl', ['$scope', '$modalInstance', 'failure', function ($scope, $modalInstance, failure) {
            $scope.failure = failure;

            $scope.close = function () {
                $modalInstance.dismiss('cancel');
            }
        }])
        .controller('CartCompareNavBarCtrl', ['$scope', '$location', 'CartCompareService', function ($scope, $location, CartCompareService) {
            $scope.$on('cart:added', function () {
                $scope.compareBenchmarkRuns = CartCompareService.getAll();
            });

            $scope.$on('cart:removed', function () {
                $scope.compareBenchmarkRuns = CartCompareService.getAll();
            });

            $scope.remove = function (benchmarkRun) {
                CartCompareService.remove(benchmarkRun);
            };

            $scope.compare = function () {
                var names = _.map($scope.compareBenchmarkRuns, function (benchmarkRun) {
                    return benchmarkRun.uniqueName;
                }).join();
                var sequenceIds = _.map($scope.compareBenchmarkRuns, function (benchmarkRun) {
                    return benchmarkRun.sequenceId;
                }).join();
                $location.path('compare/' + names + '/' + sequenceIds);
            }
        }])
        .controller('CompareCtrl', ['$scope', '$routeParams', '$filter', '$location', 'BenchmarkService', function ($scope, $routeParams, $filter, $location, BenchmarkService) {
            $scope.benchmarkRuns = [];

            var benchmarkUniqueNames = $routeParams.benchmarkNames.split(',');
            var sequenceIds = $routeParams.benchmarkSequenceIds.split(',');
            if (benchmarkUniqueNames.length != sequenceIds.length) {
                throw new Error('Expected the same number of benchmark run names and sequence ids.');
            }
            for (var i in sequenceIds) {
                BenchmarkService.loadBenchmarkRun(benchmarkUniqueNames[i], sequenceIds[i])
                    .then(function (benchmarkRun) {
                        $scope.benchmarkRuns.push(benchmarkRun);
                        prepareChartData();
                    });
            }

            var prepareChartData = function () {
                if (sequenceIds.length != $scope.benchmarkRuns.length) {
                    return; // not all benchmarkRuns are loaded yet
                }

                // sort benchmarkRuns to match requested order
                var tmpBenchmarkRuns = $scope.benchmarkRuns;
                $scope.benchmarkRuns = [];
                for (var i in sequenceIds) {
                    $scope.benchmarkRuns.push(_.findWhere(tmpBenchmarkRuns, {uniqueName: benchmarkUniqueNames[i], sequenceId: sequenceIds[i]}))
                }

                var benchmarkRunsHelper = new BenchmarkRunsHelper($scope.benchmarkRuns, []);
                $scope.aggregatedExecutionsMeasurementGraphsData = benchmarkRunsHelper.aggregatedExecutionsMeasurementGraphsData('multiBarChart', $filter, $location);
                $scope.benchmarkMeasurementGraphsData = benchmarkRunsHelper.benchmarkMeasurementGraphsData('multiBarChart', $filter, $location);
            };
        }]);
}());
