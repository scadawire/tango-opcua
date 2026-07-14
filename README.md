# tango-opcua

scadawire/tango-controls integration to OPCUA servers/devices

This device driver integrates to devices available on an OpcUA server, see also https://opcfoundation.org/about/opc-technologies/opc-ua/.

# Structure

The integration makes use of the tango python binding to implement device driver functionality.
See also https://tango-controls.readthedocs.io/en/latest/development/device-api/python/index.html

The opcua specific functionality is covered by the opcua python package.
See also https://pypi.org/project/opcua

# Attributes

Attributes are created dynamically from the `init_dynamic_attributes` device property,
which holds a json array of attribute descriptors. The attribute `name` is the opcua node id.

```json
[
  {"name": "ns=2;i=3", "data_type": "DevDouble", "unit": "bar", "write_type": "READ_WRITE"},
  {"name": "ns=2;i=4", "data_type": "DevBoolean", "label": "pump running"},
  {"name": "ns=2;i=5", "data_type": "DevLong", "data_format": "SPECTRUM"}
]
```

Supported `data_type` values are `DevBoolean`, `DevLong`, `DevFloat`, `DevDouble` and `DevString`,
supported `data_format` values are `SCALAR` (default), `SPECTRUM` and `IMAGE`. Further optional
keys are `min_value`, `max_value`, `unit`, `label`, `min_alarm`, `max_alarm`, `min_warning`,
`max_warning` and `write_type`.

If `data_type` is omitted, the tango type is derived from the type of the opcua node itself
(Boolean, the integer types, Float, Double, String). Values are converted between the tango type
and the node type on every read and write, so a node keeps its own opcua type regardless of how
the attribute is declared.

`init_dynamic_attributes` also accepts a plain comma separated list of node ids, in which case
all attributes are typed by the node type. Node ids listed in `init_subscribe` are added the
same way.

# Requirements

unknown
