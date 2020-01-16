<?php

namespace Avro\Tests;

use Avro\Exception\SchemaParseException;
use Avro\Schema\AbstractSchema;
use Avro\Schema\SchemaInterface;
use PHPUnit\Framework\TestCase;

class SchemaTest extends TestCase
{
    public function testJsonDecode()
    {
        $this->assertEquals(json_decode('null', true), null);
        $this->assertEquals(json_decode('32', true), 32);
        $this->assertEquals(json_decode('"32"', true), '32');
        $this->assertEquals((array) json_decode('{"foo": 27}'), array("foo" => 27));
        $this->assertTrue(is_array(json_decode('{"foo": 27}', true)));
        $this->assertEquals(json_decode('{"foo": 27}', true), array("foo" => 27));
        $this->assertEquals(json_decode('["bar", "baz", "blurfl"]', true), array("bar", "baz", "blurfl"));
        $this->assertFalse(is_array(json_decode('null', true)));
        $this->assertEquals(json_decode('{"type": "null"}', true), array("type" => 'null'));
        $this->assertEquals(json_decode('"boolean"'), 'boolean');
    }

    /**
     * @dataProvider parseBadJsonProvider
     */
    public function testParseBadJson($json, $failure)
    {
        if ($failure) {
            $this->expectException(SchemaParseException::class);
            $this->expectExceptionMessage($failure);
        }

        $schema = AbstractSchema::parse($json);
        $this->assertInstanceOf(SchemaInterface::class, $schema);
    }

    /**
     * @dataProvider examplesProvider
     */
    public function testParse($schemaString, $isValid, $normalizedSchemaString = null, $name = null, $comment = null)
    {
        if (!$isValid) {
            $this->expectException(SchemaParseException::class);
        }

        $schema = AbstractSchema::parse($schemaString);
        $this->assertEquals($normalizedSchemaString, strval($schema));
    }

    public function testRecordDoc()
    {
        $json = '{"type": "record", "name": "foo", "doc": "Foo doc.", "fields": [{"name": "bar", "type": "int", "doc": "Bar doc."}]}';
        $schema = AbstractSchema::parse($json);
        $this->assertEquals($schema->getDoc(), "Foo doc.");
        $fields = $schema->getFields();
        $this->assertCount(1, $fields);
        $bar = $fields[0];
        $this->assertEquals($bar->getDoc(), "Bar doc.");
    }

    public function testEnumDoc()
    {
        $json = '{"type": "enum", "name": "blood_types", "doc": "AB is freaky.", "symbols": ["A", "AB", "B", "O"]}';
        $schema = AbstractSchema::parse($json);
        $this->assertEquals($schema->getDoc(), "AB is freaky.");
    }

    protected function normalizeSchemaString($schemaString): string
    {
        return json_encode(json_decode($schemaString, true));
    }

    protected function makeExample(
        $schemaString,
        $isValid,
        $normalizedSchemaString = null,
        $name = null,
        $comment = null
    ): array
    {
        return [
            $schemaString,
            $isValid,
            $normalizedSchemaString ?: $this->normalizeSchemaString($schemaString),
            $name,
            $comment,
        ];
    }

    /**
     * @return array
     */
    public function primitiveExamplesProvider()
    {
        $examples = [];
        foreach (['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'] as $type) {
            $examples[] = $this->makeExample(sprintf('"%s"', $type), true);
            $examples[] = $this->makeExample(sprintf('{"type": "%s"}', $type), true, sprintf('"%s"', $type));
        }
        return $examples;
    }

    public function examplesProvider()
    {
        $examples = [
            $this->makeExample('"True"', false),
            $this->makeExample('{"no_type": "test"}', false),
            $this->makeExample('{"type": "panther"}', false),
            $this->makeExample('{"type": "array", "items": "long"}', true),
            $this->makeExample('{"type": "array", "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}', true),
            $this->makeExample('{"type": "map", "values": "long"}', true),
            $this->makeExample('{"type": "map", "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]}}', true),
            $this->makeExample('["string", "null", "long"]', true),
            $this->makeExample('["null", "null"]', false),
            $this->makeExample('["long", "long"]', false),
            $this->makeExample('[{"type": "array", "items": "long"}{"type": "array", "items": "string"}]', false),
            $this->makeExample('["long", {"type": "long"}, "int"]', false),
            $this->makeExample('["long", {"type": "array", "items": "long"}, {"type": "map", "values": "long"}, "int"]', true),
            $this->makeExample('["long", ["string", "null"], "int"]', false),
            $this->makeExample('["long", ["string", "null"], "int"]', false),
            $this->makeExample('["null", "boolean", "int", "long", "float", "double", "string", "bytes", {"type": "array", "items":"int"}, {"type": "map", "values":"int"}, {"name": "bar", "type":"record", "fields":[{"name":"label", "type":"string"}]}, {"name": "foo", "type":"fixed", "size":16}, {"name": "baz", "type":"enum", "symbols":["A", "B", "C"]}]', true, '["null","boolean","int","long","float","double","string","bytes",{"type":"array","items":"int"},{"type":"map","values":"int"},{"type":"record","name":"bar","fields":[{"name":"label","type":"string"}]},{"type":"fixed","name":"foo","size":16},{"type":"enum","name":"baz","symbols":["A","B","C"]}]'),
            $this->makeExample('[{"name":"subtract", "namespace":"com.example", "type":"record", "fields":[{"name":"minuend", "type":"int"}, {"name":"subtrahend", "type":"int"}]}, {"name": "divide", "namespace":"com.example", "type":"record", "fields":[{"name":"quotient", "type":"int"}, {"name":"dividend", "type":"int"}]}, {"type": "array", "items": "string"}]', true, '[{"type":"record","name":"subtract","namespace":"com.example","fields":[{"name":"minuend","type":"int"},{"name":"subtrahend","type":"int"}]},{"type":"record","name":"divide","namespace":"com.example","fields":[{"name":"quotient","type":"int"},{"name":"dividend","type":"int"}]},{"type":"array","items":"string"}]'),
            $this->makeExample('{"type": "fixed", "name": "Test", "size": 1}', true),
            $this->makeExample('{"type": "fixed", "name": "MyFixed", "namespace": "org.apache.hadoop.avro", "size": 1}', true),
            $this->makeExample('{"type": "fixed", "name": "Missing size"}', false),
            $this->makeExample('{"type": "fixed", "size": 314}', false),
            $this->makeExample('{"type":"fixed","name":"ex","doc":"this should be ignored","size": 314}', true, '{"type":"fixed","name":"ex","size":314}'),
            $this->makeExample('{"name": "bar", "namespace": "com.example", "type": "fixed", "size": 32 }', true, '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'),
            $this->makeExample('{"name": "com.example.bar", "type": "fixed", "size": 32 }', true, '{"type":"fixed","name":"bar","namespace":"com.example","size":32}'),
            $this->makeExample('{"type":"fixed","name":"_x.bar","size":4}', true, '{"type":"fixed","name":"bar","namespace":"_x","size":4}'),
            $this->makeExample('{"type":"fixed","name":"baz._x","size":4}', true, '{"type":"fixed","name":"_x","namespace":"baz","size":4}'),
            $this->makeExample('{"type":"fixed","name":"baz.3x","size":4}', false),
            $this->makeExample('{"type": "enum", "name": "Test", "symbols": ["A", "B"]}', true),
            $this->makeExample('{"type": "enum", "name": "Status", "symbols": "Normal Caution Critical"}', false),
            $this->makeExample('{"type": "enum", "name": [ 0, 1, 1, 2, 3, 5, 8 ], "symbols": ["Golden", "Mean"]}', false),
            $this->makeExample('{"type": "enum", "symbols" : ["I", "will", "fail", "no", "name"]}', false),
            $this->makeExample('{"type": "enum", "name": "Test" "symbols" : ["AA", "AA"]}', false),
            $this->makeExample('{"type":"enum","name":"Test","symbols":["AA", 16]}', false),
            $this->makeExample('{"type": "enum", "name": "blood_types", "doc": "AB is freaky.", "symbols" : ["A", "AB", "B", "O"]}', true),
            $this->makeExample('{"type": "enum", "name": "blood-types", "doc": 16, "symbols" : ["A", "AB", "B", "O"]}', false),
            $this->makeExample('{"type": "record", "name": "Test", "fields": [{"name": "f", "type": "long"}]}', true),
            $this->makeExample('{"type": "error", "name": "Test", "fields": [{"name": "f", "type": "long"}]}', true),
            $this->makeExample('{"type": "record", "name": "Node", "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}]}', true),
            $this->makeExample('{"type": "record", "name": "ListLink", "fields": [{"name": "car", "type": "int"}, {"name": "cdr", "type": "ListLink"}]}', true),
            $this->makeExample('{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string"]}]}', true),
            $this->makeExample('{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string", {"type": "record", "name": "Cons", "fields": [{"name": "car", "type": "string"}, {"name": "cdr", "type": "string"}]}]}]}', true),
            $this->makeExample('{"type": "record", "name": "Lisp", "fields": [{"name": "value", "type": ["null", "string", {"type": "record", "name": "Cons", "fields": [{"name": "car", "type": "Lisp"}, {"name": "cdr", "type": "Lisp"}]}]}]}', true),
            $this->makeExample('{"type": "record", "name": "HandshakeRequest", "namespace": "org.apache.avro.ipc", "fields": [{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true),
            $this->makeExample('{"type": "record", "name": "HandshakeRequest", "namespace": "org.apache.avro.ipc", "fields": [{"name": "clientHash", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "clientProtocol", "type": ["null", "string"]}, {"name": "serverHash", "type": "MD5"}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true),
            $this->makeExample('{"type": "record", "name": "HandshakeResponse", "namespace": "org.apache.avro.ipc", "fields": [{"name": "match", "type": {"type": "enum", "name": "HandshakeMatch", "symbols": ["BOTH", "CLIENT", "NONE"]}}, {"name": "serverProtocol", "type": ["null", "string"]}, {"name": "serverHash", "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}]}, {"name": "meta", "type": ["null", {"type": "map", "values": "bytes"}]}]}', true, '{"type":"record","name":"HandshakeResponse","namespace":"org.apache.avro.ipc","fields":[{"name":"match","type":{"type":"enum","name":"HandshakeMatch","symbols":["BOTH","CLIENT","NONE"]}},{"name":"serverProtocol","type":["null","string"]},{"name":"serverHash","type":["null",{"type":"fixed","name":"MD5","size":16}]},{"name":"meta","type":["null",{"type":"map","values":"bytes"}]}]}'),
            $this->makeExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": {"fields": [{"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'),
            $this->makeExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'),
            $this->makeExample('{"type": "record", "namespace": "org.apache.avro", "name": "Interop", "fields": [{"type": "int", "name": "intField"}, {"type": "long", "name": "longField"}, {"type": "string", "name": "stringField"}, {"type": "boolean", "name": "boolField"}, {"type": "float", "name": "floatField"}, {"type": "double", "name": "doubleField"}, {"type": "bytes", "name": "bytesField"}, {"type": "null", "name": "nullField"}, {"type": {"items": "double", "type": "array"}, "name": "arrayField"}, {"type": {"type": "map", "values": {"fields": [{"type": "string", "name": "label"}], "type": "record", "name": "Foo"}}, "name": "mapField"}, {"type": ["boolean", "double", {"items": "bytes", "type": "array"}], "name": "unionField"}, {"type": {"symbols": ["A", "B", "C"], "type": "enum", "name": "Kind"}, "name": "enumField"}, {"type": {"type": "fixed", "name": "MD5", "size": 16}, "name": "fixedField"}, {"type": {"fields": [{"type": "string", "name": "label"}, {"type": {"items": "org.apache.avro.Node", "type": "array"}, "name": "children"}], "type": "record", "name": "Node"}, "name": "recordField"}]}', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'),
            $this->makeExample('{"type": "record", "name": "ipAddr", "fields": [{"name": "addr", "type": [{"name": "IPv6", "type": "fixed", "size": 16}, {"name": "IPv4", "type": "fixed", "size": 4}]}]}', true, '{"type":"record","name":"ipAddr","fields":[{"name":"addr","type":[{"type":"fixed","name":"IPv6","size":16},{"type":"fixed","name":"IPv4","size":4}]}]}'),
            $this->makeExample('{"type": "record", "name": "Address", "fields": [{"type": "string"}, {"type": "string", "name": "City"}]}', false),
            $this->makeExample('{"type": "record", "name": "Event", "fields": [{"name": "Sponsor"}, {"name": "City", "type": "string"}]}', false),
            $this->makeExample('{"type": "record", "fields": "His vision, from the constantly passing bars," "name", "Rainer"}', false),
            $this->makeExample('{"name": ["Tom", "Jerry"], "type": "record", "fields": [{"name": "name", "type": "string"}]}', false),
            $this->makeExample('{"type":"record","name":"foo","doc":"doc string", "fields":[{"name":"bar", "type":"int", "order":"ascending", "default":1}]}', true, '{"type":"record","name":"foo","doc":"doc string","fields":[{"name":"bar","type":"int","default":1,"order":"ascending"}]}'),
            $this->makeExample('{"type":"record", "name":"foo", "doc":"doc string", "fields":[{"name":"bar", "type":"int", "order":"bad"}]}', false),
            $this->makeExample('{"type":"record","name":"foo","fields":[{"name":"bar","type":["null","string"],"default":null}]}', true, '{"type":"record","name":"foo","fields":[{"name":"bar","type":["null","string"],"default":null}]}'),
            $this->makeExample('{"type":"record","name":"foo","fields":[{"name":"bar","type":["null","string"],"doc":"Bar name."}]}', true, '{"type":"record","name":"foo","fields":[{"name":"bar","type":["null","string"],"doc":"Bar name."}]}'),
            $this->makeExample('{"type": "record", "name": "Interop", "namespace": "org.apache.avro", "fields": [{"name": "intField", "type": "int"}, {"name": "longField", "type": "long"}, {"name": "stringField", "type": "string"}, {"name": "boolField", "type": "boolean"}, {"name": "floatField", "type": "float"}, {"name": "doubleField", "type": "double"}, {"name": "bytesField", "type": "bytes"}, {"name": "nullField", "type": "null"}, {"name": "arrayField", "type": {"type": "array", "items": "double"}}, {"name": "mapField", "type": {"type": "map", "values": {"name": "Foo", "type": "record", "fields": [{"name": "label", "type": "string"}]}}}, {"name": "unionField", "type": ["boolean", "double", {"type": "array", "items": "bytes"}]}, {"name": "enumField", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B", "C"]}}, {"name": "fixedField", "type": {"type": "fixed", "name": "MD5", "size": 16}}, {"name": "recordField", "type": {"type": "record", "name": "Node", "fields": [{"name": "label", "type": "string"}, {"name": "children", "type": {"type": "array", "items": "Node"}}]}}]} ', true, '{"type":"record","name":"Interop","namespace":"org.apache.avro","fields":[{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"stringField","type":"string"},{"name":"boolField","type":"boolean"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"nullField","type":"null"},{"name":"arrayField","type":{"type":"array","items":"double"}},{"name":"mapField","type":{"type":"map","values":{"type":"record","name":"Foo","fields":[{"name":"label","type":"string"}]}}},{"name":"unionField","type":["boolean","double",{"type":"array","items":"bytes"}]},{"name":"enumField","type":{"type":"enum","name":"Kind","symbols":["A","B","C"]}},{"name":"fixedField","type":{"type":"fixed","name":"MD5","size":16}},{"name":"recordField","type":{"type":"record","name":"Node","fields":[{"name":"label","type":"string"},{"name":"children","type":{"type":"array","items":"Node"}}]}}]}'),
        ];

        return array_merge(
            $this->primitiveExamplesProvider(),
            $examples
        );
    }

    public function parseBadJsonProvider()
    {
        return [
            // Valid
            ['{"type": "array", "items": "long"}', null],
            // Trailing comma
            ['{"type": "array", "items": "long", }', "JSON decode error 4: Syntax error"],
            // Wrong quotes
            ["{'type': 'array', 'items': 'long'}", "JSON decode error 4: Syntax error"],
            // Binary data
            ["\x11\x07", "JSON decode error 3: Control character error, possibly incorrectly encoded"],
        ];
    }
}