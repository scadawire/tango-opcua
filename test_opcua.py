"""
Full integration test for Opcua.py using a python-opcua server simulator.

Tests all variable types (DevBoolean, DevLong, DevFloat, DevDouble, DevString)
as SCALAR and SPECTRUM attributes, against opcua nodes of every common variant
type (Boolean, Int16/Int32/Int64, Float, Double, String).

All conversion and I/O logic is exercised through the real Opcua class
via unbound method calls -- no code is duplicated.

Usage:
    python test_opcua.py
"""

import sys
import time
import traceback
import functools

from tango import CmdArgType, AttrWriteType, AttrDataFormat

from opcua import Client, Server, ua

from Opcua import Opcua


SIM_PORT = 14840
SIM_ENDPOINT = "opc.tcp://127.0.0.1:%d/test/server" % SIM_PORT

_server = None
_nodes = {}


# ===========================================================================
#  Lightweight state carrier -- Opcua methods only need these attributes.
# ===========================================================================

class State:
    """Carries only instance state; every method lookup falls through to Opcua."""

    def __init__(self):
        self.client = Client(SIM_ENDPOINT)
        self.dynamicAttributes = {}
        self.subscription = None
        self.subscribe_period_ms = 500
        self.max_dim_x = 256
        self.max_dim_y = 256

    def info_stream(self, msg): pass
    def debug_stream(self, msg): pass
    def warn_stream(self, msg): pass
    def error_stream(self, msg): pass
    def push_change_event(self, name, value): pass

    def __getattr__(self, name):
        attr = getattr(Opcua, name, None)
        if attr is None:
            raise AttributeError("'State' has no attribute '%s'" % name)
        if callable(attr):
            return functools.partial(attr, self)
        return attr


class FakeAttr:
    """Stands in for the tango Attr passed to read_dynamic_attr/write_dynamic_attr."""

    def __init__(self, name, write_value=None):
        self._name = name
        self._write_value = write_value
        self.value = None

    def get_name(self):
        return self._name

    def set_value(self, value):
        self.value = value

    def get_write_value(self):
        return self._write_value


def register_attr(s, name, variable_type, data_format=AttrDataFormat.SCALAR):
    """Replicate the bookkeeping add_dynamic_attribute does, minus the tango Attr."""
    s.dynamicAttributes[name] = {
        "variableType": variable_type,
        "dataFormat": data_format,
        "value": None,
    }


def read_attr(s, name):
    attr = FakeAttr(name)
    Opcua.read_dynamic_attr(s, attr)
    return attr.value


def write_attr(s, name, value):
    Opcua.write_dynamic_attr(s, FakeAttr(name, value))


# ===========================================================================
#  Simulator
# ===========================================================================

def start_server():
    global _server, _nodes
    _server = Server()
    _server.set_endpoint(SIM_ENDPOINT)
    idx = _server.register_namespace("test")
    obj = _server.get_objects_node().add_object(idx, "Demo")

    def add(name, value, variant_type):
        node = obj.add_variable(idx, name, ua.Variant(value, variant_type))
        node.set_writable()
        _nodes[name] = node.nodeid.to_string()

    add("bool", False, ua.VariantType.Boolean)
    add("int16", 0, ua.VariantType.Int16)
    add("int32", 0, ua.VariantType.Int32)
    add("int64", 0, ua.VariantType.Int64)
    add("uint32", 0, ua.VariantType.UInt32)
    add("float", 0.0, ua.VariantType.Float)
    add("double", 0.0, ua.VariantType.Double)
    add("string", "", ua.VariantType.String)
    add("int32_array", [0, 0, 0], ua.VariantType.Int32)
    add("double_array", [0.0, 0.0, 0.0], ua.VariantType.Double)
    add("bool_array", [False, False], ua.VariantType.Boolean)

    _server.start()


def stop_server():
    global _server
    if _server:
        _server.stop()
        _server = None


# ===========================================================================
#  Test helpers
# ===========================================================================

passed = 0
failed = 0
errors = []


def assert_equal(test_name, actual, expected, tolerance=None):
    global passed, failed
    if tolerance is not None:
        ok = abs(actual - expected) <= tolerance
    else:
        ok = (actual == expected)

    if ok:
        passed += 1
        print("  PASS  %s" % test_name)
    else:
        failed += 1
        msg = "  FAIL  %s: expected %r, got %r" % (test_name, expected, actual)
        print(msg)
        errors.append(msg)


def assert_true(test_name, value):
    assert_equal(test_name, value, True)


def assert_false(test_name, value):
    assert_equal(test_name, value, False)


def assert_raises(test_name, fn, *args):
    global passed, failed
    try:
        fn(*args)
        failed += 1
        msg = "  FAIL  %s: expected exception" % test_name
        print(msg)
        errors.append(msg)
    except Exception:
        passed += 1
        print("  PASS  %s raises" % test_name)


# ===========================================================================
#  Pure conversion tests (no server needed)
# ===========================================================================

def test_string_value_to_var_type():
    print("\n-- stringValueToVarType --")
    s = State()
    for name, expected in [
        ("DevBoolean", CmdArgType.DevBoolean),
        ("DevLong", CmdArgType.DevLong),
        ("DevDouble", CmdArgType.DevDouble),
        ("DevFloat", CmdArgType.DevFloat),
        ("DevString", CmdArgType.DevString),
        ("", CmdArgType.DevString),
    ]:
        assert_equal("varType '%s'" % name, Opcua.stringValueToVarType(s, name), expected)
    assert_raises("varType invalid", Opcua.stringValueToVarType, s, "DevInvalid")


def test_string_value_to_write_type():
    print("\n-- stringValueToWriteType --")
    s = State()
    for name, expected in [
        ("READ", AttrWriteType.READ),
        ("WRITE", AttrWriteType.WRITE),
        ("READ_WRITE", AttrWriteType.READ_WRITE),
        ("READ_WITH_WRITE", AttrWriteType.READ_WITH_WRITE),
        ("", AttrWriteType.READ_WRITE),
    ]:
        assert_equal("writeType '%s'" % name, Opcua.stringValueToWriteType(s, name), expected)
    assert_raises("writeType invalid", Opcua.stringValueToWriteType, s, "BOGUS")


def test_string_value_to_format_type():
    print("\n-- stringValueToFormatType --")
    s = State()
    for name, expected in [
        ("SCALAR", AttrDataFormat.SCALAR),
        ("SPECTRUM", AttrDataFormat.SPECTRUM),
        ("IMAGE", AttrDataFormat.IMAGE),
        ("", AttrDataFormat.SCALAR),
    ]:
        assert_equal("dataFormat '%s'" % name, Opcua.stringValueToFormatType(s, name), expected)
    assert_raises("dataFormat invalid", Opcua.stringValueToFormatType, s, "MATRIX")


def test_variant_type_to_var_type_name():
    print("\n-- variantTypeToVarTypeName --")
    s = State()
    for variant, expected in [
        (ua.VariantType.Boolean, "DevBoolean"),
        (ua.VariantType.SByte, "DevLong"),
        (ua.VariantType.Byte, "DevLong"),
        (ua.VariantType.Int16, "DevLong"),
        (ua.VariantType.UInt16, "DevLong"),
        (ua.VariantType.Int32, "DevLong"),
        (ua.VariantType.UInt32, "DevLong"),
        (ua.VariantType.Int64, "DevLong"),
        (ua.VariantType.UInt64, "DevLong"),
        (ua.VariantType.Float, "DevFloat"),
        (ua.VariantType.Double, "DevDouble"),
        (ua.VariantType.String, "DevString"),
        (ua.VariantType.DateTime, "DevString"),
    ]:
        assert_equal("variant %s" % variant.name,
                     Opcua.variantTypeToVarTypeName(s, variant), expected)


def test_parse_boolean():
    print("\n-- parseBoolean --")
    s = State()
    assert_true("bool True", Opcua.parseBoolean(s, True))
    assert_false("bool False", Opcua.parseBoolean(s, False))
    assert_true("bool 1", Opcua.parseBoolean(s, 1))
    assert_false("bool 0", Opcua.parseBoolean(s, 0))
    assert_true("bool 2.5", Opcua.parseBoolean(s, 2.5))
    assert_true("bool 'true'", Opcua.parseBoolean(s, "true"))
    assert_true("bool 'True'", Opcua.parseBoolean(s, "True"))
    assert_true("bool '1'", Opcua.parseBoolean(s, "1"))
    assert_true("bool 'yes'", Opcua.parseBoolean(s, "yes"))
    assert_false("bool 'false'", Opcua.parseBoolean(s, "false"))
    assert_false("bool '0'", Opcua.parseBoolean(s, "0"))
    assert_false("bool ''", Opcua.parseBoolean(s, ""))


def test_scalar_conversions():
    print("\n-- scalarToTypeValue --")
    s = State()
    assert_equal("bool from int", Opcua.scalarToTypeValue(s, 1, CmdArgType.DevBoolean), True)
    assert_equal("long from float", Opcua.scalarToTypeValue(s, 3.9, CmdArgType.DevLong), 3)
    assert_equal("long from string", Opcua.scalarToTypeValue(s, "42", CmdArgType.DevLong), 42)
    assert_equal("double from string", Opcua.scalarToTypeValue(s, "3.14", CmdArgType.DevDouble), 3.14)
    assert_equal("float from int", Opcua.scalarToTypeValue(s, 7, CmdArgType.DevFloat), 7.0)
    assert_equal("string from float", Opcua.scalarToTypeValue(s, 1.5, CmdArgType.DevString), "1.5")
    assert_raises("unsupported var type", Opcua.scalarToTypeValue, s, 1, CmdArgType.DevShort)


def test_array_conversions():
    print("\n-- arrayToTypeValue --")
    s = State()
    assert_equal("long array", Opcua.arrayToTypeValue(s, [1.7, 2.2], CmdArgType.DevLong), [1, 2])
    assert_equal("double array", Opcua.arrayToTypeValue(s, ["1.5", 2], CmdArgType.DevDouble), [1.5, 2.0])
    assert_equal("bool array", Opcua.arrayToTypeValue(s, [1, 0], CmdArgType.DevBoolean), [True, False])
    assert_equal("nested image", Opcua.arrayToTypeValue(s, [[1, 0], [0, 1]], CmdArgType.DevBoolean),
                 [[True, False], [False, True]])
    assert_equal("empty array", Opcua.arrayToTypeValue(s, [], CmdArgType.DevLong), [])


def test_type_value_to_node_value():
    print("\n-- typeValueToNodeValue --")
    s = State()
    assert_equal("to Boolean from 'true'",
                 Opcua.typeValueToNodeValue(s, "true", ua.VariantType.Boolean), True)
    assert_equal("to Int32 from '5'",
                 Opcua.typeValueToNodeValue(s, "5", ua.VariantType.Int32), 5)
    assert_equal("to Int32 from 5.9",
                 Opcua.typeValueToNodeValue(s, 5.9, ua.VariantType.Int32), 5)
    assert_equal("to Double from '2.5'",
                 Opcua.typeValueToNodeValue(s, "2.5", ua.VariantType.Double), 2.5)
    assert_equal("to String from 3",
                 Opcua.typeValueToNodeValue(s, 3, ua.VariantType.String), "3")
    assert_equal("to Int32 list",
                 Opcua.typeValueToNodeValue(s, ["1", 2.7], ua.VariantType.Int32), [1, 2])


def test_default_type_value():
    print("\n-- defaultTypeValue --")
    s = State()
    register_attr(s, "d_bool", CmdArgType.DevBoolean)
    register_attr(s, "d_long", CmdArgType.DevLong)
    register_attr(s, "d_double", CmdArgType.DevDouble)
    register_attr(s, "d_string", CmdArgType.DevString)
    register_attr(s, "d_spectrum", CmdArgType.DevLong, AttrDataFormat.SPECTRUM)

    assert_equal("default bool", Opcua.defaultTypeValue(s, "d_bool"), False)
    assert_equal("default long", Opcua.defaultTypeValue(s, "d_long"), 0)
    assert_equal("default double", Opcua.defaultTypeValue(s, "d_double"), 0.0)
    assert_equal("default string", Opcua.defaultTypeValue(s, "d_string"), "")
    assert_equal("default spectrum", Opcua.defaultTypeValue(s, "d_spectrum"), [])


# ===========================================================================
#  Integration tests -- against the opcua server
# ===========================================================================

def test_node_type_inference(s):
    print("\n-- node type inference --")
    for node, expected in [
        ("bool", "DevBoolean"),
        ("int16", "DevLong"),
        ("int32", "DevLong"),
        ("int64", "DevLong"),
        ("uint32", "DevLong"),
        ("float", "DevFloat"),
        ("double", "DevDouble"),
        ("string", "DevString"),
    ]:
        assert_equal("infer %s" % node, Opcua.nodeVarTypeName(s, _nodes[node]), expected)

    # unknown node must not raise, it falls back to DevString
    assert_equal("infer unknown node", Opcua.nodeVarTypeName(s, "ns=2;i=99999"), "DevString")


def test_scalar_roundtrips(s):
    print("\n-- scalar read/write roundtrips --")

    register_attr(s, _nodes["bool"], CmdArgType.DevBoolean)
    for val in [True, False]:
        write_attr(s, _nodes["bool"], val)
        assert_equal("Boolean node %s" % val, read_attr(s, _nodes["bool"]), val)

    register_attr(s, _nodes["int32"], CmdArgType.DevLong)
    for val in [0, 1, -1, 2147483647, -2147483648]:
        write_attr(s, _nodes["int32"], val)
        assert_equal("Int32 node %s" % val, read_attr(s, _nodes["int32"]), val)

    register_attr(s, _nodes["int16"], CmdArgType.DevLong)
    for val in [0, 32767, -32768]:
        write_attr(s, _nodes["int16"], val)
        assert_equal("Int16 node %s" % val, read_attr(s, _nodes["int16"]), val)

    register_attr(s, _nodes["int64"], CmdArgType.DevLong)
    write_attr(s, _nodes["int64"], 123456789)
    assert_equal("Int64 node", read_attr(s, _nodes["int64"]), 123456789)

    register_attr(s, _nodes["uint32"], CmdArgType.DevLong)
    write_attr(s, _nodes["uint32"], 4294967295)
    assert_equal("UInt32 node max", read_attr(s, _nodes["uint32"]), 4294967295)

    register_attr(s, _nodes["float"], CmdArgType.DevFloat)
    for val in [0.0, 1.5, -0.25]:
        write_attr(s, _nodes["float"], val)
        assert_equal("Float node %s" % val, read_attr(s, _nodes["float"]), val, tolerance=1e-6)

    register_attr(s, _nodes["double"], CmdArgType.DevDouble)
    for val in [0.0, 2.718281828459045, -1e100]:
        write_attr(s, _nodes["double"], val)
        assert_equal("Double node %s" % val, read_attr(s, _nodes["double"]), val,
                     tolerance=max(abs(val) * 1e-12, 1e-9))

    register_attr(s, _nodes["string"], CmdArgType.DevString)
    for val in ["", "Hello", "25°C", "äöü"]:
        write_attr(s, _nodes["string"], val)
        assert_equal("String node '%s'" % val, read_attr(s, _nodes["string"]), val)


def test_cross_type_coercion(s):
    print("\n-- cross-type coercion (tango type != node type) --")

    # DevString attribute on a Double node: write must still land as a Double
    name = _nodes["double"]
    register_attr(s, name, CmdArgType.DevString)
    write_attr(s, name, "12.5")
    assert_equal("DevString -> Double node", read_attr(s, name), "12.5")
    assert_equal("Double node holds float", _server.get_node(name).get_value(), 12.5)

    # DevString attribute on a Boolean node
    name = _nodes["bool"]
    register_attr(s, name, CmdArgType.DevString)
    write_attr(s, name, "true")
    assert_equal("Boolean node holds bool", _server.get_node(name).get_value(), True)

    # DevLong attribute on a Double node
    name = _nodes["double"]
    register_attr(s, name, CmdArgType.DevLong)
    write_attr(s, name, 7)
    assert_equal("DevLong -> Double node", read_attr(s, name), 7)

    # DevDouble attribute on an Int32 node truncates on write
    name = _nodes["int32"]
    register_attr(s, name, CmdArgType.DevDouble)
    write_attr(s, name, 9.9)
    assert_equal("DevDouble -> Int32 node", read_attr(s, name), 9.0)


def test_spectrum_roundtrips(s):
    print("\n-- spectrum read/write roundtrips --")

    name = _nodes["int32_array"]
    register_attr(s, name, CmdArgType.DevLong, AttrDataFormat.SPECTRUM)
    write_attr(s, name, [1, 2, 3])
    assert_equal("Int32 array", read_attr(s, name), [1, 2, 3])

    name = _nodes["double_array"]
    register_attr(s, name, CmdArgType.DevDouble, AttrDataFormat.SPECTRUM)
    write_attr(s, name, [1.5, -2.25, 0.0])
    assert_equal("Double array", read_attr(s, name), [1.5, -2.25, 0.0])

    name = _nodes["bool_array"]
    register_attr(s, name, CmdArgType.DevBoolean, AttrDataFormat.SPECTRUM)
    write_attr(s, name, [True, False])
    assert_equal("Boolean array", read_attr(s, name), [True, False])


def test_read_before_subscription(s):
    print("\n-- read falls back to a direct node read --")
    # seed the node behind the driver's back, cache is still None
    _server.get_node(_nodes["double"]).set_value(ua.Variant(42.5, ua.VariantType.Double))
    name = _nodes["double"]
    register_attr(s, name, CmdArgType.DevDouble)  # resets cached value to None
    assert_equal("uncached read hits the server", read_attr(s, name), 42.5, tolerance=1e-9)


def test_datachange_notification(s):
    print("\n-- datachange_notification --")
    name = _nodes["int32"]
    register_attr(s, name, CmdArgType.DevLong)

    class FakeNode:
        class nodeid:
            @staticmethod
            def to_string():
                return name

    Opcua.datachange_notification(s, FakeNode, 77, None)
    assert_equal("notification updates cache", s.dynamicAttributes[name]["value"], 77)
    assert_equal("notification value reads back typed", read_attr(s, name), 77)


# ===========================================================================
#  Main
# ===========================================================================

def main():
    global failed

    test_string_value_to_var_type()
    test_string_value_to_write_type()
    test_string_value_to_format_type()
    test_variant_type_to_var_type_name()
    test_parse_boolean()
    test_scalar_conversions()
    test_array_conversions()
    test_type_value_to_node_value()
    test_default_type_value()

    print("\n== Starting opcua server simulator on port %d ==" % SIM_PORT)
    try:
        start_server()
    except Exception as e:
        print("FATAL: Cannot start opcua server: %s" % e)
        traceback.print_exc()
        sys.exit(1)

    time.sleep(0.5)

    try:
        s = State()
        s.client.connect()
        print("Connected to opcua server\n")

        test_node_type_inference(s)
        test_scalar_roundtrips(s)
        test_cross_type_coercion(s)
        test_spectrum_roundtrips(s)
        test_read_before_subscription(s)
        test_datachange_notification(s)

        s.client.disconnect()
    except Exception:
        traceback.print_exc()
        failed += 1
    finally:
        stop_server()

    total = passed + failed
    print("\n" + "=" * 50)
    print("  Results: %d/%d passed, %d failed" % (passed, total, failed))
    if errors:
        print("\n  Failures:")
        for e in errors:
            print("    %s" % e)
    print("=" * 50)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
