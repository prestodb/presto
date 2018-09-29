# Presto Ranger Plugin

Presto Ranger Plugin is a `SystemAccessControl` implementations,  this plugin adds Apache Ranger integration support for Presto.

## Configuration

As documentation of [system-access-control](https://prestodb.io/docs/current/develop/system-access-control.html) mentioned,  it is configured using an `etc/access-control.properties` file.The `access-control.name` and some ranger property must set in `access-control.properties`.

Example configuration file:

```properties
access-control.name=ranger-access-control
ranger.service.store.rest.url=http://localhost:6080
ranger.service.store.rest.username=admin
ranger.service.store.rest.password=admin
ranger.plugin.presto.policy.rest.url=http://localhost:6080
ranger.plugin.presto.policy.cache.dir=/tmp/presto/policycache
ranger.plugin.presto.service.name=presto
ranger.plugin.presto.policy.source.impl=org.apache.ranger.admin.client.RangerAdminRESTClient
```



A ranger plugin for presto must be add in your ranger admin. 

This plugin must have 4 resources, `catalog`,`database`,`table` and `column`. 

Documentation of [How to add a custom plugin](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=53741207) is a good starting point.

Example ranger plugin json:

```json
{
    "description": "PRESTO Component",
    "enums": [],
    "implClass": "com.xxx.ranger.presto.service.RangerServicePresto",
    "isEnabled": true,
    "label": "PRESTO",
    "name": "presto",
    "options": {},
    "policyConditions": [],
    "accessTypes": [
        {
            "impliedGrants": [],
            "itemId": 1,
            "label": "use",
            "name": "use"
        },
         {
            "impliedGrants": ["use"],
            "itemId": 2,
            "label": "select",
            "name": "select"
        },
         {
            "impliedGrants": ["use"],
            "itemId": 3,
            "label": "create",
            "name": "create"
        },
         {
            "impliedGrants": ["use","select"],
            "itemId": 4,
            "label": "update",
            "name": "update"
        },
         {
            "impliedGrants": ["use"],
            "itemId": 5,
            "label": "drop",
            "name": "drop"
        },
         {
            "impliedGrants": ["drop","update","create","select","use"],
            "itemId": 6,
            "label": "admin",
            "name": "admin"
        }
    ],
    "configs": [
        {
            "itemId": 1,
            "label": "JdbcUrl",
            "mandatory": true,
            "name": "jdbcUrl",
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        },
        {
            "itemId": 2,
            "label": "Username",
            "mandatory": true,
            "name": "username",
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        },
        {
            "itemId": 3,
            "label": "Password",
            "mandatory": false,
            "name": "password",
            "type": "password",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        }
    ],
    "contextEnrichers": [],
    "resources": [
        {
            "description": "Catalog",
            "excludesSupported": true,
            "itemId": 1,
            "label": "Catalog",
            "level": 10,
            "lookupSupported": true,
            "mandatory": true,
            "matcher": "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher",
            "matcherOptions": {
                "ignoreCase": "true",
                "wildCard": "true"
            },
            "name": "catalog",
            "recursiveSupported": false,
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        },
        {
            "description": "Database",
            "excludesSupported": true,
            "itemId": 2,
            "label": "Database",
            "level": 20,
            "lookupSupported": true,
            "mandatory": true,
            "matcher": "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher",
            "matcherOptions": {
                "ignoreCase": "true",
                "wildCard": "true"
            },
            "name": "database",
            "parent":"catalog",
            "recursiveSupported": false,
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        },
        {
            "description": "Table",
            "excludesSupported": true,
            "itemId": 3,
            "label": "Table",
            "level": 30,
            "lookupSupported": true,
            "mandatory": true,
            "matcher": "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher",
            "matcherOptions": {
                "ignoreCase": "true",
                "wildCard": "true"
            },
            "name": "table",
            "parent":"database",
            "recursiveSupported": false,
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        },
        {
            "description": "Column",
            "excludesSupported": true,
            "itemId": 4,
            "label": "Column",
            "level": 40,
            "lookupSupported": true,
            "mandatory": true,
            "matcher": "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher",
            "matcherOptions": {
                "ignoreCase": "true",
                "wildCard": "true"
            },
            "name": "column",
            "parent":"table",
            "recursiveSupported": false,
            "type": "string",
            "uiHint": "",
            "validationMessage": "",
            "validationRegEx": ""
        }
    ]
}
```

