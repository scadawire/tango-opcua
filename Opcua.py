# see also: https://github.com/FreeOpcUa/python-opcua/blob/master/examples/client_to_kepware.py
# see also: https://python-opcua.readthedocs.io/en/latest/client.html

import time
import os
import json
from json import JSONDecodeError
from tango import AttrQuality, AttrWriteType, AttrDataFormat, DispLevel, DevState
from tango import Attr, SpectrumAttr, ImageAttr, CmdArgType, UserDefaultAttrProp
from tango.server import Device, attribute, command, DeviceMeta
from tango.server import class_property, device_property
from tango.server import run
from opcua import Client, ua

class Opcua(Device, metaclass=DeviceMeta):

    host = device_property(dtype=str, default_value="127.0.0.1")
    path = device_property(dtype=str, default_value="unknown/unknown")
    port = device_property(dtype=int, default_value=4840)
    init_subscribe = device_property(dtype=str, default_value="")
    init_dynamic_attributes = device_property(dtype=str, default_value="")
    username = device_property(dtype=str, default_value="")
    password = device_property(dtype=str, default_value="")
    subscribe_period_ms = device_property(dtype=int, default_value=500)
    max_dim_x = device_property(dtype=int, default_value=256)
    max_dim_y = device_property(dtype=int, default_value=256)
    client = 0
    subscription = None
    dynamicAttributes = {}

    INTEGER_VARIANT_TYPES = (
        ua.VariantType.SByte, ua.VariantType.Byte,
        ua.VariantType.Int16, ua.VariantType.UInt16,
        ua.VariantType.Int32, ua.VariantType.UInt32,
        ua.VariantType.Int64, ua.VariantType.UInt64,
    )

    @attribute
    def time(self):
        return time.time()

    def on_connect(self):
        self.info_stream("Connected")
        self.set_state(DevState.ON)
        for key in self.dynamicAttributes:
            self.subscribe(key)

    # called by native opcua subscribe callback
    def datachange_notification(self, node, val, data):
        name = node.nodeid.to_string()
        self.info_stream("Received message: " + name + " " + str(val))
        if name not in self.dynamicAttributes:
            self.add_dynamic_attribute(name)
        if self.dynamicAttributes[name]["value"] != val:
            self.dynamicAttributes[name]["value"] = val
            self.push_change_event(name, self.valueToTypeValue(name, val))

    def event_notification(self, event):
        self.info_stream("New event " + str(event))

    @command(dtype_in=str)
    def add_dynamic_attribute(self, topic,
            variable_type_name="", min_value="", max_value="",
            unit="", write_type_name="", label="", min_alarm="", max_alarm="",
            min_warning="", max_warning="", data_format_name=""):
        if topic == "": return
        if topic in self.dynamicAttributes:
            self.info_stream("Dynamic attribute already exists: " + topic)
            return
        # the server knows the node type, so use it whenever none was configured
        if variable_type_name == "":
            variable_type_name = self.nodeVarTypeName(topic)
        variableType = self.stringValueToVarType(variable_type_name)
        writeType = self.stringValueToWriteType(write_type_name)
        dataFormat = self.stringValueToFormatType(data_format_name)
        prop = UserDefaultAttrProp()
        if(min_value != "" and min_value != max_value): prop.set_min_value(min_value)
        if(max_value != "" and min_value != max_value): prop.set_max_value(max_value)
        if(unit != ""): prop.set_unit(unit)
        if(label != ""): prop.set_label(label)
        if(min_alarm != ""): prop.set_min_alarm(min_alarm)
        if(max_alarm != ""): prop.set_max_alarm(max_alarm)
        if(min_warning != ""): prop.set_min_warning(min_warning)
        if(max_warning != ""): prop.set_max_warning(max_warning)

        if dataFormat == AttrDataFormat.SPECTRUM:
            attr = SpectrumAttr(topic, variableType, writeType, self.max_dim_x)
        elif dataFormat == AttrDataFormat.IMAGE:
            attr = ImageAttr(topic, variableType, writeType, self.max_dim_x, self.max_dim_y)
        else:
            attr = Attr(topic, variableType, writeType)
        attr.set_default_properties(prop)
        self.add_attribute(attr, r_meth=self.read_dynamic_attr, w_meth=self.write_dynamic_attr)
        self.set_change_event(topic, True, False)
        self.dynamicAttributes[topic] = {
            "variableType": variableType,
            "dataFormat": dataFormat,
            "value": None,
        }
        self.info_stream("added dynamic attribute " + topic)

    def stringValueToVarType(self, variable_type_name) -> CmdArgType:
        mapping = {
            "DevBoolean": CmdArgType.DevBoolean,
            "DevLong": CmdArgType.DevLong,
            "DevDouble": CmdArgType.DevDouble,
            "DevFloat": CmdArgType.DevFloat,
            "DevString": CmdArgType.DevString,
            "": CmdArgType.DevString,
        }
        if variable_type_name not in mapping:
            raise Exception("given variable_type '" + variable_type_name +
                "' unsupported, supported are: DevBoolean, DevLong, DevDouble, DevFloat, DevString")
        return mapping[variable_type_name]

    def stringValueToWriteType(self, write_type_name) -> AttrWriteType:
        mapping = {
            "READ": AttrWriteType.READ,
            "WRITE": AttrWriteType.WRITE,
            "READ_WRITE": AttrWriteType.READ_WRITE,
            "READ_WITH_WRITE": AttrWriteType.READ_WITH_WRITE,
            "": AttrWriteType.READ_WRITE,
        }
        if write_type_name not in mapping:
            raise Exception("given write_type '" + write_type_name +
                "' unsupported, supported are: READ, WRITE, READ_WRITE, READ_WITH_WRITE")
        return mapping[write_type_name]

    def stringValueToFormatType(self, format_type_name) -> AttrDataFormat:
        mapping = {
            "SCALAR": AttrDataFormat.SCALAR,
            "SPECTRUM": AttrDataFormat.SPECTRUM,
            "IMAGE": AttrDataFormat.IMAGE,
            "": AttrDataFormat.SCALAR,
        }
        if format_type_name not in mapping:
            raise Exception("given data_format '" + format_type_name +
                "' unsupported, supported are: SCALAR, SPECTRUM, IMAGE")
        return mapping[format_type_name]

    def variantTypeToVarTypeName(self, variant_type) -> str:
        if variant_type == ua.VariantType.Boolean:
            return "DevBoolean"
        if variant_type in self.INTEGER_VARIANT_TYPES:
            return "DevLong"
        if variant_type == ua.VariantType.Float:
            return "DevFloat"
        if variant_type == ua.VariantType.Double:
            return "DevDouble"
        return "DevString"

    def nodeVarTypeName(self, topic) -> str:
        try:
            return self.variantTypeToVarTypeName(self.nodeVariantType(topic))
        except Exception as e:
            self.warn_stream("Failed to resolve node type of " + topic + ", assuming DevString: " + str(e))
            return "DevString"

    def nodeVariantType(self, topic):
        return self.client.get_node(topic).get_data_type_as_variant_type()

    def parseBoolean(self, value) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in ("true", "1", "yes", "on")
        return bool(value)

    def valueToTypeValue(self, name, value):
        """Cast a value coming from the opcua server into the tango attribute type."""
        lookup = self.dynamicAttributes[name]
        variableType = lookup["variableType"]
        dataFormat = lookup["dataFormat"]
        if value is None:
            return self.defaultTypeValue(name)
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="ignore")
        if dataFormat != AttrDataFormat.SCALAR:
            if isinstance(value, str):
                value = json.loads(value) if value != "" else []
            return self.arrayToTypeValue(value, variableType)
        return self.scalarToTypeValue(value, variableType)

    def scalarToTypeValue(self, value, variableType):
        if variableType == CmdArgType.DevBoolean:
            return self.parseBoolean(value)
        if variableType == CmdArgType.DevLong:
            return int(float(value))
        if variableType in (CmdArgType.DevDouble, CmdArgType.DevFloat):
            return float(value)
        if variableType == CmdArgType.DevString:
            return str(value)
        raise Exception("Unsupported variable type: " + str(variableType))

    def arrayToTypeValue(self, values, variableType):
        if len(values) > 0 and isinstance(values[0], (list, tuple)):
            return [[self.scalarToTypeValue(v, variableType) for v in row] for row in values]
        return [self.scalarToTypeValue(v, variableType) for v in values]

    def defaultTypeValue(self, name):
        lookup = self.dynamicAttributes[name]
        if lookup["dataFormat"] != AttrDataFormat.SCALAR:
            return []
        variableType = lookup["variableType"]
        if variableType == CmdArgType.DevBoolean:
            return False
        if variableType == CmdArgType.DevLong:
            return 0
        if variableType in (CmdArgType.DevDouble, CmdArgType.DevFloat):
            return 0.0
        return ""

    def typeValueToNodeValue(self, value, variant_type):
        """Cast a tango write value into the type the opcua node expects."""
        if isinstance(value, (list, tuple)):
            return [self.typeValueToNodeValue(v, variant_type) for v in value]
        if variant_type == ua.VariantType.Boolean:
            return self.parseBoolean(value)
        if variant_type in self.INTEGER_VARIANT_TYPES:
            return int(float(value))
        if variant_type in (ua.VariantType.Float, ua.VariantType.Double):
            return float(value)
        if variant_type == ua.VariantType.String:
            return str(value)
        return value

    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        value = self.dynamicAttributes[name]["value"]
        if value is None:
            # no subscription update received yet, read the node directly
            try:
                value = self.client.get_node(name).get_value()
                self.dynamicAttributes[name]["value"] = value
            except Exception as e:
                self.error_stream("Failed to read node " + name + ": " + str(e))
        self.debug_stream("read value " + str(name) + ": " + str(value))
        attr.set_value(self.valueToTypeValue(name, value))

    def write_dynamic_attr(self, attr):
        name = attr.get_name()
        value = attr.get_write_value()
        if self.dynamicAttributes[name]["dataFormat"] != AttrDataFormat.SCALAR:
            value = value.tolist() if hasattr(value, "tolist") else list(value)
        # cache what the node actually stores, not what was written: the node type
        # may narrow the value (a double written to an Int32 node lands truncated)
        written = self.write_node(name, value)
        self.dynamicAttributes[name]["value"] = written
        self.push_change_event(name, self.valueToTypeValue(name, written))

    def write_node(self, topic, value):
        node = self.client.get_node(topic)
        variant_type = None
        try:
            variant_type = node.get_data_type_as_variant_type()
        except Exception as e:
            self.warn_stream("Failed to resolve node type of " + topic + ": " + str(e))
        if variant_type is None:
            node.set_value(value)
            return value
        value = self.typeValueToNodeValue(value, variant_type)
        node.set_value(value, variant_type)
        return value

    @command(dtype_in=str)
    def subscribe(self, topic):
        self.info_stream("Subscribe to topic " + str(topic))
        if self.subscription is None:
            self.subscription = self.client.create_subscription(self.subscribe_period_ms, self)
        self.subscription.subscribe_data_change(self.client.get_node(topic))

    @command(dtype_in=[str])
    def publish(self, args):
        topic, value = args
        self.info_stream("Publish topic " + str(topic) + ": " + str(value))
        self.write_node(topic, value)

    def reconnect(self):
        self.subscription = None
        self.client.connect()
        self.on_connect()

    def init_device(self):
        self.set_state(DevState.INIT)
        self.get_device_properties(self.get_device_class())
        connectionString = "opc.tcp://" + self.host + ":" + str(self.port) + "/" + self.path
        self.info_stream("Connecting to " + connectionString)
        self.client = Client(connectionString)
        if self.username != "": self.client.set_user(self.username)
        if self.password != "": self.client.set_password(self.password)
        # connect first, so that node types can be resolved while adding attributes
        try:
            self.client.connect()
        except Exception as e:
            self.error_stream("Failed to connect to " + connectionString + ": " + str(e))
            self.set_state(DevState.FAULT)

        if self.init_dynamic_attributes != "":
            try:
                attributes = json.loads(self.init_dynamic_attributes)
                for attributeData in attributes:
                    self.add_dynamic_attribute(attributeData["name"],
                        attributeData.get("data_type", ""), attributeData.get("min_value", ""),
                        attributeData.get("max_value", ""), attributeData.get("unit", ""),
                        attributeData.get("write_type", ""), attributeData.get("label", ""),
                        attributeData.get("min_alarm", ""), attributeData.get("max_alarm", ""),
                        attributeData.get("min_warning", ""), attributeData.get("max_warning", ""),
                        attributeData.get("data_format", ""))
            except JSONDecodeError:
                attributes = self.init_dynamic_attributes.split(",")
                for attribute in attributes:
                    self.info_stream("Init dynamic attribute: " + str(attribute.strip()))
                    self.add_dynamic_attribute(attribute.strip())
        if self.init_subscribe != "":
            init_subscribes = self.init_subscribe.split(",")
            for init_subscribe in init_subscribes:
                self.info_stream("Init subscribe: " + str(init_subscribe.strip()))
                self.add_dynamic_attribute(init_subscribe.strip())

        if self.get_state() != DevState.FAULT:
            self.on_connect()

if __name__ == "__main__":
    deviceServerName = os.getenv("DEVICE_SERVER_NAME")
    run({deviceServerName: Opcua})
