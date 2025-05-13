//The MIT License (MIT)
//
//Copyright (c) 2015 Tableau
//
//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:
//
//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.
(function() {

    var versionNumber = "1.1.0";
    var _sourceWindow;

    if (typeof tableauVersionBootstrap === 'undefined') {
        // tableau version bootstrap isn't defined. We are likely running in the simulator so init up our tableau object
        tableau = {
            connectionName: "",
            connectionData: "",
            password: "",
            username: "",
            incrementalExtractColumn: "",

            initCallback: function () {
                _sendMessage("initCallback");
            },

            shutdownCallback: function () {
                _sendMessage("shutdownCallback");
            },

            submit: function () {
                _sendMessage("submit");
            },

            log: function (msg) {
                _sendMessage("log", {"logMsg": msg});
            },

            headersCallback: function (fieldNames, types) {
                _sendMessage("headersCallback", {"fieldNames": fieldNames, "types":types});
            },

            dataCallback: function (data, lastRecordToken, moreData) {
                _sendMessage("dataCallback", {"data": data, "lastRecordToken": lastRecordToken, "moreData": moreData});
            },

            abortWithError: function (errorMsg) {
                _sendMessage("abortWithError", {"errorMsg": errorMsg});
            }
        };
    } else { // Tableau version bootstrap is defined. Let's use it
        tableauVersionBootstrap.ReportVersionNumber(versionNumber);
    }

    // Check if something weird happened during bootstraping. If so, just define a tableau object to we don't
    // throw errors all over the place because tableau isn't defined
    if (typeof tableau === "undefined") {
        tableau = {}
    }

    tableau.versionNumber = versionNumber;

    tableau.phaseEnum = {
        interactivePhase: "interactive", 
        authPhase: "auth",
        gatherDataPhase: "gatherData"
    };

    if (!tableau.phase) {
        tableau.phase = tableau.phaseEnum.interactivePhase;
    }

    // Assign the functions we always want to have available on the tableau object
    tableau.makeConnector = function() {
        var defaultImpls = {
            init: function() { tableau.initCallback(); },
            shutdown: function() { tableau.shutdownCallback(); }
        };
        return defaultImpls;
    };

    tableau.registerConnector = function (wdc) {
        // do some error checking on the wdc
        var functionNames = ["init", "shutdown", "getColumnHeaders", "getTableData"]
        for (var ii = functionNames.length - 1; ii >= 0; ii--) {
            if (typeof(wdc[functionNames[ii]]) !== "function") {
                throw "The connector did not define the required function: " + functionNames[ii];
            }
        };
        window._wdc = wdc;
    };

    function _sendMessage(msgName, msgData) {
        var messagePayload = _buildMessagePayload(msgName, msgData);
        
        // Check first to see if we have a messageHandler defined to post the message to
        if (typeof window.webkit != 'undefined' &&
            typeof window.webkit.messageHandlers != 'undefined' &&
            typeof window.webkit.messageHandlers.wdcHandler != 'undefined') {
            
            window.webkit.messageHandlers.wdcHandler.postMessage(messagePayload);
        } else if (!_sourceWindow) {
            throw "Looks like the WDC is calling a tableau function before tableau.init() has been called."
        } else {
            _sourceWindow.postMessage(messagePayload, "*");
        }
    }

    function _buildMessagePayload(msgName, msgData) {
        var msgObj = {"msgName": msgName,
                      "props": _packagePropertyValues(),
                      "msgData": msgData};
        return JSON.stringify(msgObj);
    }

    function _packagePropertyValues() {
        var propValues = {"connectionName": tableau.connectionName,
                          "connectionData": tableau.connectionData,
                          "password": tableau.password,
                          "username": tableau.username,
                          "incrementalExtractColumn": tableau.incrementalExtractColumn,
                          "versionNumber": tableau.versionNumber};
        return propValues;
    }

    function _applyPropertyValues(props) {
        if (props) {
            tableau.connectionName = props.connectionName;
            tableau.connectionData = props.connectionData;
            tableau.password = props.password;
            tableau.username = props.username;
            tableau.incrementalExtractColumn = props.incrementalExtractColumn;
        }
    }

    function _receiveMessage(evnt) {
        var wdc = window._wdc;
        if (!wdc) {
            throw "No WDC registered. Did you forget to call tableau.registerConnector?";
        }
        if (!_sourceWindow) {
            _sourceWindow = evnt.source
        }
        var payloadObj = JSON.parse(evnt.data);
        var msgData = payloadObj.msgData;
        _applyPropertyValues(payloadObj.props);

        switch(payloadObj.msgName) {
            case "init":
                tableau.phase = msgData.phase;
                wdc.init();
            break;
            case "shutdown":
                wdc.shutdown();
            break;
            case "getColumnHeaders":
                wdc.getColumnHeaders();
            break;
            case "getTableData":
                wdc.getTableData(msgData.lastRecordToken);
            break;
        }
    };

    // Add global error handler. If there is a javascript error, this will report it to Tableau
    // so that it can be reported to the user.
    window.onerror = function (message, file, line, column, errorObj) {
        if (tableau._hasAlreadyThrownErrorSoDontThrowAgain) {
            return true;
        }
        var msg = message;
        if(errorObj) {
            msg += "   stack:" + errorObj.stack;
        } else {
            msg += "   file: " + file;
            msg += "   line: " + line;
        }

        if (tableau && tableau.abortWithError) {
            tableau.abortWithError(msg);
        } else {
            throw msg;
        }
        tableau._hasAlreadyThrownErrorSoDontThrowAgain = true;
        return true;
    }

    window.addEventListener('message', _receiveMessage, false);
})();