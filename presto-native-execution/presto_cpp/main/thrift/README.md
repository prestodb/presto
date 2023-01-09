This is the Thrift serde used to communicate with the Presto coordinator.

The end state for this code will eventually be just the presto_thrift.thrift
file and its fbthrift generated code.  While in the transition from JSON to
Thrift we will need code to convert between the two internal data structures
(JSON derrived and Thrift derrived) that presto_cpp will be using.

There is currently only one Thrift root class `TaskStatus` being used to return
the results of the .getTaskStatus endpoint.  To accomplish this the JSON
derrived `TaskStatus` must to converted to its corrosponding struct in Thrift.
This code gen produces a toThrift function for each Thrift structure that is
also in the JSON protocol.

<pre><code>
presto_thrift.thrift  ---> fbthrift ---> $BUILDDIR/presto_cpp/main/thrift/ProtocolToThrift.[h|cpp]
        |
        v
   thrift2json.py
        |
        v
 presto_thrift.json     presto_protocol/presto_protocol.json  presto_protocol-to-thrift-json.yml
        |                               |                                     |
        |                               v                                     |
        +------------->   presto_protocol-to-thrift-json.py   <---------------+
                                        |
                                        v
                          presto_protocol-to-thrift-json.json
                                  |              |
ProtocolToThrift-cpp.mustache     |              |      ProtocolToThrift-hpp.mustache
               |                  |              |                  |
               v                  |              v                  |
            chevron <-------------+           chevron <-------------+
               |                                 |
               v                                 v
     ProtocolToThrift.cpp                ProtocolToThrift.cpp
</code></pre>

`thrift2json.py` uses ptsd_jbroll a fork of the ptsd Thrift parser to produce a
JSON representation of the `presto_thrift.thrift` file.  This is combined with
the `presto_protocol.json` from the presto_protocol code gen by
`presto_protocol-to-thrift-json.py` to create a JSON description of the
conversion from JSON internal srtruct to Thrift structs in
`presto_protocol-to-thrift-json.json`.

`presto_protocol-to-thrift-json.json` is used by chevron templates
`ProtocolToThrift-cpp.mustache` and `ProtocolToThrift-hpp.mustache` to produce
the actual C++ code in `ProtocolToThrift.cpp` and `ProtocolToThrift.cpp`.
