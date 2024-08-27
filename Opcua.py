# see also: https://github.com/FreeOpcUa/python-opcua/blob/master/examples/client_to_kepware.py
# see also: https://python-opcua.readthedocs.io/en/latest/client.html

import time
from tango import AttrQuality, AttrWriteType, DispLevel, DevState, Attr, CmdArgType
from tango.server import Device, attribute, command, DeviceMeta
from tango.server import class_property, device_property
from tango.server import run
import os
from opcua import Client

class Opcua(Device, metaclass=DeviceMeta):
    pass

    host = device_property(dtype=str, default_value="127.0.0.1")
    path = device_property(dtype=str, default_value="unknown/unknown")
    port = device_property(dtype=int, default_value=4840)
    init_subscribe = device_property(dtype=str, default_value="")
    init_dynamic_attributes = device_property(dtype=str, default_value="")
    username = device_property(dtype=str, default_value="")
    password = device_property(dtype=str, default_value="")
    client = 0
    dynamicAttributes = {}

    @attribute
    def time(self):
        return time.time()

    def on_connect(self):
        self.info_stream("Connected")
        self.set_state(DevState.ON)
        for key in self.dynamicAttributes:
            self.subscribe(key)

    def datachange_notification(self, node, val, data):
        topic = node.nodeid.to_string()
        payload = val
        self.info_stream("Received message: " + topic +" "+str(payload))
        if not topic in self.dynamicAttributes:
            self.add_dynamic_attribute(topic)
        self.dynamicAttributes[topic] = str(payload)
        self.push_change_event(topic, str(payload))

    def event_notification(self, event):
        print("Python: New event", event)
    
    # TODO: need to be implemented above...
    #def on_message(self, client, userdata, msg):
    #    self.info_stream("Received message: " + msg.topic+" "+str(msg.payload))
    #    if not msg.topic in self.dynamicAttributes:
    #        self.add_dynamic_attribute(msg.topic)
    #    self.dynamicAttributes[msg.topic] = msg.payload
    #    self.push_change_event(msg.topic, msg.payload)

    @command(dtype_in=str)
    def add_dynamic_attribute(self, topic):
        if topic == "": return
        attr = Attr(topic, CmdArgType.DevString, AttrWriteType.READ_WRITE)
        self.add_attribute(attr, r_meth=self.read_dynamic_attr, w_meth=self.write_dynamic_attr)
        self.dynamicAttributes[topic] = ""

    def read_dynamic_attr(self, attr):
        attr.set_value(self.dynamicAttributes[attr.get_name()])

    def write_dynamic_attr(self, attr):
        self.dynamicAttributes[attr.get_name()] = attr.get_write_value()
        self.publish([attr.get_name(), self.dynamicAttributes[attr.get_name()]])

    @command(dtype_in=str)
    def subscribe(self, topic):
        self.info_stream("Subscribe to topic " + str(topic))
        sub = self.client.create_subscription(500, self)
        sub.subscribe_data_change(self.client.get_node(topic))

    @command(dtype_in=[str])
    def publish(self, args):
        topic, value = args
        self.info_stream("Publish topic " + str(topic) + ": " + str(value))
        # self.client.publish(topic, value)
        root = self.client.get_root_node()
        node = self.client.get_node(topic)
        node.set_value(value)

    def reconnect(self):
        self.client.connect()
        root = self.client.get_root_node()
        print("Root is", root)
        print("childs of root are: ", root.get_children())
        print("name of root is", root.get_browse_name())
        objects = self.client.get_objects_node()
        print("childs og objects are: ", objects.get_children())
        self.on_connect()
        
    def init_device(self):
        self.set_state(DevState.INIT)
        self.get_device_properties(self.get_device_class())
        connectionString = "opc.tcp://" + self.host + ":" + str(self.port) + "/" + self.path
        self.info_stream("Connecting to " + connectionString)
        self.client = Client(connectionString)
        if self.username != "":
            client.set_user(self.username)
        if self.password != "":
            client.set_password(password)
        if self.init_dynamic_attributes != "":
            attributes = self.init_dynamic_attributes.split(",")
            for attribute in attributes:
                self.info_stream("Init dynamic attribute: " + str(attribute.strip()))
                self.add_dynamic_attribute(attribute.strip())
        if self.init_subscribe != "":
            init_subscribes = self.init_subscribe.split(",")
            for init_subscribe in init_subscribes:
                self.info_stream("Init subscribe: " + str(init_subscribe.strip()))
                self.add_dynamic_attribute(init_subscribe.strip())
        self.reconnect()
        
        # from IPython import embed
        # embed()

if __name__ == "__main__":
    deviceServerName = os.getenv("DEVICE_SERVER_NAME")
    run({deviceServerName: Opcua})