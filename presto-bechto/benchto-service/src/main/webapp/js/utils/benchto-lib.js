/*
 * Copyright 2013-2016, Teradata, Inc. All rights reserved.
 */
var CartHelper = (function () {
    'use strict';

    return {
        setCartAddedFlag: function (CartCompareService, benchmarkRuns) {
            _.forEach(benchmarkRuns, function (benchmarkRun) {
                benchmarkRun.addedToCompare = CartCompareService.contains(benchmarkRun);
            });
        },
        toggleCartAddedFlag: function (CartCompareService, benchmarkRuns, changedBenchmarkRun, addedToCompate) {
            var sameBenchmarkRun = CartCompareService.findInCollection(benchmarkRuns, changedBenchmarkRun);
            if (sameBenchmarkRun) {
                sameBenchmarkRun.addedToCompare = addedToCompate;
            }
        },
        updateBenchmarkCartSelection: function (CartCompareService, changedBenchmarkRun) {
            if (changedBenchmarkRun.addedToCompare) {
                CartCompareService.add(changedBenchmarkRun);
            }
            else {
                CartCompareService.remove(changedBenchmarkRun);
            }
        }
    };
}).call('CartHelper');

var BenchmarkRunsHelper = function (benchmarkRuns, tags) {
    'use strict';

    this.benchmarkRuns = benchmarkRuns;
    this.tags = tags;

    this.aggregatedExecutionsMeasurementKeys = function () {
        return _.uniq(_.flatten(_.map(this.benchmarkRuns,
            function (benchmarkRun) {
                return _.allKeys(benchmarkRun.aggregatedMeasurements);
            }
        )));
    };

    this.aggregatedExecutionsMeasurementUnit = function (measurementKey) {
        for (var i = 0; i < this.benchmarkRuns.length; ++i) {
            var aggregatedMeasurement = this.benchmarkRuns[i].aggregatedMeasurements[measurementKey];
            if (aggregatedMeasurement) {
                return aggregatedMeasurement.unit;
            }
        }
        throw new Error("Could not find unit for measurement key: " + measurementKey);
    };

    this.extractAggregatedExecutionsAggregatedMeasurements = function (measurementKey) {
        return _.map(this.benchmarkRuns, function (benchmarkRun) {
            return benchmarkRun.aggregatedMeasurements[measurementKey];
        });
    };

// benchmark measurements graph data
    this.benchmarkMeasurementKeys = function () {
        return _.chain(this.benchmarkRuns)
            .map(function (benchmarkRun) { return _.pluck(benchmarkRun.measurements, 'name'); })
            .flatten()
            .uniq()
            .value();
    };

    this.benchmarkMeasurementUnit = function (measurementKey) {
        for (var i = 0; i < this.benchmarkRuns.length; ++i) {
            var measurement = _.findWhere(this.benchmarkRuns[i].measurements, {name: measurementKey});
            if (measurement) {
                return measurement.unit;
            }
        }
        throw new Error("Could not find unit for measurement key: " + measurementKey);
    };

    this.extractBenchmarkMeasurements = function (measurementKey) {
        return _.map(this.benchmarkRuns, function (benchmarkRun) {
            return _.findWhere(benchmarkRun.measurements, {name: measurementKey});
        });
    };

    this.dataForSingleMeasurementKey = function (measurements, singleMeasurement) {
        return {
            "key": singleMeasurement,
            "values": _.zip(
                _.map(_.range(this.benchmarkRuns.length), function (i) { return i + 1; }),
                _.pluck(measurements, singleMeasurement)
            )
        };
    };

    // Add tag points to time series, so tag get displayed on charts
    this.dataForTags = function() {
        var values = [];
        var latestTag = -1;
        for (var benchmarkId = 0; benchmarkId < this.benchmarkRuns.length; benchmarkId++) {
            var changed = false;
            for (var tagId = latestTag + 1; tagId < this.tags.length; tagId++) {
                if (this.benchmarkRuns[benchmarkId].started >= this.tags[tagId].created) {
                    latestTag = tagId;
                    changed = true;
                } else {
                    break;
                }
            }
            if (changed) {
                values.push([benchmarkId + 1, 0]);
            }
        }
        return {
            key: "tag",
            values: values,
            strokeWidth: 0.1,
        };
    };

    this.dataForAggregatedMeasurementKey = function (measurementKey) {
        var aggregatedMeasurements = this.extractAggregatedExecutionsAggregatedMeasurements(measurementKey);
        var data = [
            this.dataForSingleMeasurementKey(aggregatedMeasurements, 'mean'),
            this.dataForSingleMeasurementKey(aggregatedMeasurements, 'min'),
            this.dataForSingleMeasurementKey(aggregatedMeasurements, 'max'),
            this.dataForSingleMeasurementKey(aggregatedMeasurements, 'stdDev')
        ];
        if (this.tags !== undefined && this.tags.length > 0) {
            data.push(this.dataForTags());
        }
        return data;
    };

    this.aggregatedExecutionsMeasurementGraphsData = function (chartType, $filter, $location) {
        var benchmarkRuns = this.benchmarkRuns;
        var tags = this.tags;
        return _.map(this.aggregatedExecutionsMeasurementKeys(), function (measurementKey) {
            var super_this = new BenchmarkRunsHelper(benchmarkRuns, tags);
            var unit = super_this.aggregatedExecutionsMeasurementUnit(measurementKey);
            var data = super_this.dataForAggregatedMeasurementKey(measurementKey);
            return {
                data: data,
                options: super_this.optionsFor(data, chartType, measurementKey, unit, $filter, $location, benchmarkRuns, tags)
            }
        });
    };

    this.benchmarkMeasurementGraphsData = function (chartType, $filter, $location) {
        var benchmarkRuns = this.benchmarkRuns;
        var tags = this.tags;
        return _.map(this.benchmarkMeasurementKeys(), function (measurementKey) {
            var super_this = new BenchmarkRunsHelper(benchmarkRuns, tags);
            var unit = super_this.benchmarkMeasurementUnit(measurementKey);
            var measurements = super_this.extractBenchmarkMeasurements(measurementKey);
            var data = [super_this.dataForSingleMeasurementKey(measurements, "value")];
            return {
                data: data,
                options: super_this.optionsFor(data, chartType, measurementKey, unit, $filter, $location, benchmarkRuns, tags)
            };
        });
    };

    this.optionsFor = function (data, chartType, measurementKey, unit, $filter, $location, benchmarkRuns, tags) {
        var maxY = _.chain(data)
            .map(function (singleData) {
                return _.map(singleData.values, function (values) {
                    return values[1];
                });
            })
            .flatten()
            .max()
            .value();

        var filteredMaxYWithUnit;
        if (measurementKey == 'duration') {
            filteredMaxYWithUnit = $filter('duration')(maxY);
        }
        else {
            filteredMaxYWithUnit = $filter('unit')(maxY, unit);
        }

        var filteredMaxY = parseFloat(filteredMaxYWithUnit);
        var valueScaleFactor = filteredMaxY / maxY;
        unit = filteredMaxYWithUnit.split(' ')[1];

        var yAxisTickFormat = function(d) {
            return d3.format('.01f')(d);
        };
        var scaleYValue = function(d) {
            if (d === undefined) {
                return undefined;
            }
            return d[1] * valueScaleFactor;
        };
        var valueFormatter = function(d) {
            return yAxisTickFormat(scaleYValue(d)) + ' ' + unit;
        };
        var indexFromChartObject = function(chartObject) {
            if (chartObject.value !== null && chartObject.value !== undefined) {
                return chartObject.value - 1;
            }
            if (chartObject.series.key == 'tag') {
                return undefined;
            }
            var index = chartObject.index;
            if (index === null || index === undefined) {
                index = chartObject.pointIndex;
            }
            return index;
        };
        var onElementClick = function(e) {
            var benchmarkRun = benchmarkRuns[indexFromChartObject(e)];
            var angular_root_element = document.getElementById('angular_root_element');
            angular.element(angular_root_element).scope().$apply(function () {
                $location.path('benchmark/' + benchmarkRun.uniqueName + "/" + benchmarkRun.sequenceId)
            });
        };
        return {
            chart: {
                type: chartType,
                height: 250,
                width: null,
                x: function (d) {
                    return d[0];
                },
                y: scaleYValue,
                yDomain: [0, filteredMaxY],
                stacked: false,
                yAxis: {
                    axisLabel: unit,
                    tickFormat: yAxisTickFormat
                },
                xAxis: {
                    axisLabel: 'benchmark execution id',
                },
                tooltip: {
                    enabled: true,
                    contentGenerator: function(obj) {
                        return Jaml.render('tooltip', {
                            index: indexFromChartObject(obj),
                            benchmarkRuns: benchmarkRuns,
                            tags: tags,
                            data: data,
                            valueFormatter: valueFormatter
                        });
                    }
                },
                multibar: {
                  dispatch: {
                    elementClick: onElementClick
                  }
                },
                lines: {
                  dispatch: {
                    elementClick: onElementClick
                  }
                }
            },
            title: {
                enable: true,
                text: measurementKey
            }
        };
    };

    return this;
};

Jaml.register('tooltip_entry', function(entry) {
    tr(td(entry.key), td(entry.value));
});

Jaml.register('tooltip', function(params) {
    var benchmarkRun = params.benchmarkRuns[params.index];
    var tag = _.chain(params.tags)
        .filter(function(tag) { return tag.created <= benchmarkRun.started; })
        .max(function(tag) {return tag.created;})
        .value();
    var tagInfo = "";
    if(typeof tag.name !== "undefined") {
        tagInfo = "<br/>Latest tag: " + tag.name;
    }

    var entries = _.chain(params.data)
        .filter(function(dataEntry) { return dataEntry.key != 'tag' })
        .map(function (dataEntry) {
            return {
                key: dataEntry.key,
                value: params.valueFormatter(dataEntry.values[params.index])
            };
        }).value();
    table(
        thead(tr(
            td({colspan: 2},
                strong({class: 'x-value'}, benchmarkRun.name),
                " (" + benchmarkRun.sequenceId + ")",
                 tagInfo
            )
        )),
        tbody(
            Jaml.render('tooltip_entry', entries)
        )
    )
});
