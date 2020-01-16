<?php

namespace Avro\Tests;

use Avro\Exception\SchemaParseException;
use Avro\Protocol\Factory;
use Avro\Protocol\Protocol;
use PHPUnit\Framework\TestCase;

class ProtocolTest extends TestCase
{
    /**
     * @dataProvider dataProvider
     */
    public function testParsing($parseable, $data)
    {
        if (!$parseable) {
            $this->expectException(SchemaParseException::class);
        }

        $protocol = Factory::parseProtocol($data);
        $this->assertInstanceOf(Protocol::class, $protocol);
    }

    public function dataProvider()
    {
        return [
            [
                true,
                '{
                  "namespace": "com.acme",
                  "protocol": "HelloWorld",
                
                  "types": [
                    {"name": "Greeting", "type": "record", "fields": [
                      {"name": "message", "type": "string"}]},
                    {"name": "Curse", "type": "error", "fields": [
                      {"name": "message", "type": "string"}]}
                  ],
                
                  "messages": {
                    "hello": {
                      "request": [{"name": "greeting", "type": "Greeting" }],
                      "response": "Greeting",
                      "errors": ["Curse"]
                    }
                  }
                }',
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test",
                 "protocol": "Simple",
                
                 "types": [
                     {"name": "Kind", "type": "enum", "symbols": ["FOO","BAR","BAZ"]},
                
                     {"name": "MD5", "type": "fixed", "size": 16},
                
                     {"name": "TestRecord", "type": "record",
                      "fields": [
                          {"name": "name", "type": "string", "order": "ignore"},
                          {"name": "kind", "type": "Kind", "order": "descending"},
                          {"name": "hash", "type": "MD5"}
                      ]
                     },
                
                     {"name": "TestError", "type": "error", "fields": [
                         {"name": "message", "type": "string"}
                      ]
                     }
                
                 ],
                
                 "messages": {
                
                     "hello": {
                         "request": [{"name": "greeting", "type": "string"}],
                         "response": "string"
                     },
                
                     "echo": {
                         "request": [{"name": "record", "type": "TestRecord"}],
                         "response": "TestRecord"
                     },
                
                     "add": {
                         "request": [{"name": "arg1", "type": "int"}, {"name": "arg2", "type": "int"}],
                         "response": "int"
                     },
                
                     "echoBytes": {
                         "request": [{"name": "data", "type": "bytes"}],
                         "response": "bytes"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["TestError"]
                     }
                 }
                
                }'
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test.namespace",
                 "protocol": "TestNamespace",
                
                 "types": [
                     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                     {"name": "TestRecord", "type": "record",
                      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"} ]
                     },
                     {"name": "TestError", "namespace": "org.apache.avro.test.errors",
                      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
                     }
                 ],
                
                 "messages": {
                     "echo": {
                         "request": [{"name": "record", "type": "TestRecord"}],
                         "response": "TestRecord"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["org.apache.avro.test.errors.TestError"]
                     }
                
                 }
                
                }'
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test.namespace",
                 "protocol": "TestImplicitNamespace",
                
                 "types": [
                     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                     {"name": "ReferencedRecord", "type": "record", 
                       "fields": [ {"name": "foo", "type": "string"} ] },
                     {"name": "TestRecord", "type": "record",
                      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                                  {"name": "unqalified", "type": "ReferencedRecord"} ]
                     },
                     {"name": "TestError",
                      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
                     }
                 ],
                
                 "messages": {
                     "echo": {
                         "request": [{"name": "qualified", 
                             "type": "org.apache.avro.test.namespace.TestRecord"}],
                         "response": "TestRecord"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["org.apache.avro.test.namespace.TestError"]
                     }
                
                 }
                
                }'
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test.namespace",
                 "protocol": "TestNamespaceTwo",
                
                 "types": [
                     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                     {"name": "ReferencedRecord", "type": "record", 
                       "namespace": "org.apache.avro.other.namespace", 
                       "fields": [ {"name": "foo", "type": "string"} ] },
                     {"name": "TestRecord", "type": "record",
                      "fields": [ {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                                  {"name": "qualified", 
                                    "type": "org.apache.avro.other.namespace.ReferencedRecord"} 
                                ]
                     },
                     {"name": "TestError",
                      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
                     }
                 ],
                
                 "messages": {
                     "echo": {
                         "request": [{"name": "qualified", 
                             "type": "org.apache.avro.test.namespace.TestRecord"}],
                         "response": "TestRecord"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["org.apache.avro.test.namespace.TestError"]
                     }
                
                 }
                
                }'
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test.namespace",
                 "protocol": "TestValidRepeatedName",
                
                 "types": [
                     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                     {"name": "ReferencedRecord", "type": "record", 
                       "namespace": "org.apache.avro.other.namespace", 
                       "fields": [ {"name": "foo", "type": "string"} ] },
                     {"name": "ReferencedRecord", "type": "record", 
                       "fields": [ {"name": "bar", "type": "double"} ] },
                     {"name": "TestError",
                      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
                     }
                 ],
                
                 "messages": {
                     "echo": {
                         "request": [{"name": "qualified", 
                             "type": "ReferencedRecord"}],
                         "response": "org.apache.avro.other.namespace.ReferencedRecord"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["org.apache.avro.test.namespace.TestError"]
                     }
                
                 }
                
                }'
            ],
            [
                false,
                '{"namespace": "org.apache.avro.test.namespace",
                 "protocol": "TestInvalidRepeatedName",
                
                 "types": [
                     {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                     {"name": "ReferencedRecord", "type": "record", 
                       "fields": [ {"name": "foo", "type": "string"} ] },
                     {"name": "ReferencedRecord", "type": "record", 
                       "fields": [ {"name": "bar", "type": "double"} ] },
                     {"name": "TestError",
                      "type": "error", "fields": [ {"name": "message", "type": "string"} ]
                     }
                 ],
                
                 "messages": {
                     "echo": {
                         "request": [{"name": "qualified", 
                             "type": "ReferencedRecord"}],
                         "response": "org.apache.avro.other.namespace.ReferencedRecord"
                     },
                
                     "error": {
                         "request": [],
                         "response": "null",
                         "errors": ["org.apache.avro.test.namespace.TestError"]
                     }
                
                 }
                
                }'
            ],
            [
                true,
                '{"namespace": "org.apache.avro.test",
                 "protocol": "BulkData",
                
                 "types": [],
                
                 "messages": {
                
                     "read": {
                         "request": [],
                         "response": "bytes"
                     },
                
                     "write": {
                         "request": [ {"name": "data", "type": "bytes"} ],
                         "response": "null"
                     }
                
                 }
                
                }'
            ],
            [
                true,
                '{
                  "protocol" : "API",
                  "namespace" : "xyz.api",
                  "types" : [ {
                    "type" : "enum",
                    "name" : "Symbology",
                    "namespace" : "xyz.api.product",
                    "symbols" : [ "OPRA", "CUSIP", "ISIN", "SEDOL" ]
                  }, {
                    "type" : "record",
                    "name" : "Symbol",
                    "namespace" : "xyz.api.product",
                    "fields" : [ {
                      "name" : "symbology",
                      "type" : "xyz.api.product.Symbology"
                    }, {
                      "name" : "symbol",
                      "type" : "string"
                    } ]
                  }, {
                    "type" : "record",
                    "name" : "MultiSymbol",
                    "namespace" : "xyz.api.product",
                    "fields" : [ {
                      "name" : "symbols",
                      "type" : {
                        "type" : "map",
                        "values" : "xyz.api.product.Symbol"
                      }
                    } ]
                  } ],
                  "messages" : {
                  }
                }'
            ]
        ];
    }
}