/**
        Tableau web connector doc says that it supports: bool, date, datetime, float, int, and string
        So, fields with complex types are converted to json strings.
*/
function StatementClient(connectionData, headerCallback, dataCallback, errorCallback) {
    this.query = connectionData.query;
    this.catalog = connectionData.catalog;
    this.schema = connectionData.schema;
    this.source = 'Tableau Web Connector';
    this.user = connectionData.userName;
    this.sessionParameters = [];

    this.headerCallback = headerCallback;
    this.dataCallback = dataCallback;
    this.errorCallback = errorCallback;

    this.currentResults = null;
    this.valid = true;

    if (!(connectionData.sessionParameters === undefined)) {
        var parameterMap = JSON.parse(connectionData.sessionParameters);
        for (var name in parameterMap) {
          var value = parameterMap[name];
          this.sessionParameters.push(name + '=' + value);
        }
    }

    this.headers = {
        "X-Presto-User": this.user ? this.user : 'N/A',
        "X-Presto-Source": this.source,
        "X-Presto-Session": this.sessionParameters
    };

    if (!(this.catalog === undefined)) {
        this.headers["X-Presto-Catalog"] = this.catalog
    }

    if (!(this.schema === undefined)) {
        this.headers["X-Presto-Schema"] = this.schema
    }

    // lastRecordNumber starts with 0 according to Tableau web connector docs
    this.submitQuery(0);
}

StatementClient.prototype.submitQuery = function(lastRecordNumber) {
    var statementClient = this;
    $.ajax({
        type: "POST",
        url: '/v1/statement',
        headers: this.headers,
        data: this.query,
        dataType: 'json',
        // FIXME having problems when async: true
        async: false,
        error: function(xhr, statusStr, errorStr) {
            statementClient.errorCallback(xhr.responseText, errorStr);
        },
        success: function(response) {
            statementClient.currentResults = response;
            statementClient.responseHandler(response, lastRecordNumber);
        }
    });
};

StatementClient.prototype.advance = function(lastRecordNumber) {
    if (!this.currentResults || !this.currentResults.nextUri) {
        this.valid = false;
        return;
    }

    var statementClient = this;
    $.ajax({
        type: "GET",
        url: this.currentResults.nextUri,
        headers: this.headers,
        dataType: 'json',
        // FIXME having problems when async: true
        async: false,
        error: function(xhr, statusStr, errorStr) {
            statementClient.errorCallback(xhr.responseText, errorStr);
        },
        success: function(response) {
            statementClient.currentResults = response;
            statementClient.responseHandler(response, lastRecordNumber);
            if (!(response.data || response.error)) {
                // no data in this batch, schedule another GET
                statementClient.advance(lastRecordNumber);
            }
        }
    });
};

StatementClient.prototype.responseHandler = function(response, lastRecordNumber) {
    if (response.error) {
        this.errorCallback(response.error.errorName, response.error.message);
    }

    if (response.columns) {
        this.headerCallback(response.columns);
    }

    if (response.data) {
        // push the columns first in case we didn't get columns in previous requests
        this.headerCallback(response.columns);
        this.dataCallback(response.data, response.columns, lastRecordNumber);
    }
}
