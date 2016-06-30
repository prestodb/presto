/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
(function () {
    'use strict';

    angular.module('benchmarkServiceUI.services', [])
        .factory('BenchmarkService', ['$http', '$q', function ($http, $q) {
            var sortMeasurements = function (measurements) {
                return _.sortBy(measurements, function (measurement) {
                    // we want duration measurement to go first in UI
                    if (measurement.name == 'duration') {
                        // special characters are before alphanumeric ascii codes
                        return '!';
                    }
                    return measurement.name;
                });
            };

            var postProcessBenchmarkRun = function (benchmarkRun) {
                benchmarkRun.executions = _.sortBy(benchmarkRun.executions, function (execution) {
                    var sequenceId = execution.sequenceId;
                    var length = sequenceId.length;
                    if (length == 1) {
                        return '00' + sequenceId
                    }
                    else if (length == 2) {
                        return '0' + sequenceId
                    }
                    else if (length == 3) {
                        return sequenceId;
                    }
                    else {
                        throw 'Too long sequence to be well sorted: ' + sequenceId;
                    }
                });
                benchmarkRun.measurements = sortMeasurements(benchmarkRun.measurements);
                _.each(benchmarkRun.executions, function (execution) {
                    execution.measurements = sortMeasurements(execution.measurements);
                });

                // convert to angular date filter consumable format
                benchmarkRun.started = benchmarkRun.started * 1000;
                benchmarkRun.ended = benchmarkRun.ended * 1000;
            };

            return {
                loadBenchmarkRun: function (uniqueName, benchmarkSequenceId) {
                    var deferredBenchmark = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/benchmark/' + uniqueName + '/' + benchmarkSequenceId
                    }).then(function (response) {
                        var benchmarkRun = response.data;
                        postProcessBenchmarkRun(benchmarkRun);
                        deferredBenchmark.resolve(benchmarkRun);
                    }, function (reason) {
                        deferredBenchmark.reject(reason);
                    });
                    return deferredBenchmark.promise;
                },
                loadBenchmark: function (uniqueName, environmentName) {
                    var deferredBenchmark = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/benchmark/' + uniqueName,
                        params: {
                            environment: environmentName
                        }
                    }).then(function (response) {
                        var runs = response.data;
                        runs.forEach(function (benchmarkRun) {
                            postProcessBenchmarkRun(benchmarkRun);
                        });
                        deferredBenchmark.resolve(runs);
                    }, function (reason) {
                        deferredBenchmark.reject(reason);
                    });
                    return deferredBenchmark.promise;
                },
                loadLatestBenchmarkRuns: function (environmentName) {
                    var deferredBenchmarks = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/benchmark/latest/' + environmentName
                    }).then(function (response) {
                        var benchmarkRuns = response.data;
                        benchmarkRuns.forEach(function (benchmarkRun) {
                            postProcessBenchmarkRun(benchmarkRun);
                        });
                        deferredBenchmarks.resolve(benchmarkRuns);
                    }, function (reason) {
                        deferredBenchmarks.reject(reason);
                    });
                    return deferredBenchmarks.promise;
                }
            };
        }])
        .factory('EnvironmentService', ['$http', '$q', function ($http, $q) {
            return {
                loadEnvironment: function (environmentName) {
                    var deferredEnvironment = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/environment/' + environmentName
                    }).then(function (response) {
                        deferredEnvironment.resolve(response.data);
                    }, function (reason) {
                        deferredEnvironment.reject(reason);
                    });
                    return deferredEnvironment.promise;
                },
                loadEnvironments: function () {
                    var deferredEnvironments = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/environments/'
                    }).then(function (response) {
                        deferredEnvironments.resolve(response.data);
                    }, function (reason) {
                        deferredEnvironments.reject(reason);
                    });
                    return deferredEnvironments.promise;
                }
            };
        }])
        .factory('CartCompareService', ['$rootScope', function ($rootScope) {
            var cart = [];
            return {
                add: function (benchmarkRun) {
                    cart.push(benchmarkRun);
                    $rootScope.$broadcast('cart:added', benchmarkRun);
                },
                remove: function (benchmarkRun) {
                    cart.splice(cart.indexOf(benchmarkRun), 1);
                    $rootScope.$broadcast('cart:removed', benchmarkRun);
                },
                getAll: function () {
                    return cart;
                },
                size: function () {
                    return cart.length;
                },
                findInCollection: function (benchmarkRuns, searchBenchmarkRun) {
                    for (var i in benchmarkRuns) {
                        if (searchBenchmarkRun.uniqueName == benchmarkRuns[i].uniqueName && searchBenchmarkRun.sequenceId == benchmarkRuns[i].sequenceId) {
                            return benchmarkRuns[i];
                        }
                    }
                    return undefined;
                },
                contains: function (benchmarkRun) {
                    return this.findInCollection(cart, benchmarkRun) ? true : false;
                }
            };
        }])
        .factory('TagService', ['$http', '$q', function ($http, $q) {
            return {
                loadTags: function (environmentName) {
                    var deferredEnvironment = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/tags/' + environmentName
                    }).then(function (response) {
                        deferredEnvironment.resolve(response.data);
                    }, function (reason) {
                        deferredEnvironment.reject(reason);
                    });
                    return deferredEnvironment.promise;
                },
                loadTags: function (environmentName, start, end) {
                    var deferredEnvironment = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/tags/' + environmentName,
                        params: {
                            start: start,
                            end: end
                        }
                    }).then(function (response) {
                        var tags = response.data;
                        tags.forEach(function (tag) {
                            // convert to angular date filter consumable format
                            tag.created *= 1000;
                        });
                        deferredEnvironment.resolve(tags);
                    }, function (reason) {
                        deferredEnvironment.reject(reason);
                    });
                    return deferredEnvironment.promise;
                },
                loadLatest: function (environmentName, until) {
                    var deferredEnvironment = $q.defer();
                    $http({
                        method: 'GET',
                        url: '/v1/tags/' + environmentName + '/latest?until=' + until
                    }).then(function (response) {
                        deferredEnvironment.resolve(response.data);
                    }, function (reason) {
                        deferredEnvironment.reject(reason);
                    });
                    return deferredEnvironment.promise;
                }
            };
        }]);
}());
