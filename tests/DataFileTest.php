<?php

namespace Avro\Tests;

use Avro\DataFile\DataIO;
use Avro\Tests\Traits\DataFilesTrait;
use PHPUnit\Framework\TestCase;

class DataFileTest extends TestCase
{
    use DataFilesTrait;

    protected function setUp(): void
    {
        $this->setupTmpDir();
    }

    protected function tearDown(): void
    {
        $this->removeDataFiles();
    }

    public function testWriteReadNothingRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-nothing-null.avr');
        $writersSchema = '"null"';
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $data = $dr->data();
        $dr->close();
        $this->assertEmpty($data);
    }

    public function testWriteReadNullRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-null.avr');
        $writersSchema = '"null"';
        $data = null;
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->append($data);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $readData = reset($readData);
        $dr->close();
        $this->assertEquals($data, $readData);
    }

    public function testWriteReadStringRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-str.avr');
        $writersSchema = '"string"';
        $data = 'foo';
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->append($data);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $readData = reset($readData);
        $dr->close();
        $this->assertEquals($data, $readData);
    }

    public function testWriteReadRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-int.avr');
        $writersSchema = '"int"';
        $data = 1;

        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->append(1);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $readData = reset($readData);
        $dr->close();
        $this->assertEquals($data, $readData);

    }

    public function testWriteReadTrueRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-true.avr');
        $writersSchema = '"boolean"';
        $datum = true;
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->append($datum);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $readData = reset($readData);
        $dr->close();
        $this->assertEquals($datum, $readData);
    }

    public function testWriteReadFalseRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-false.avr');
        $writersSchema = '"boolean"';
        $datum = false;
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        $dw->append($datum);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $readData = reset($readData);
        $dr->close();
        $this->assertEquals($datum, $readData);
    }

    public function testWriteReadIntArrayRoundTrip()
    {
        $dataFile = $this->addDataFile('data-wr-int-ary.avr');
        $writersSchema = '"int"';
        $data = array(10, 20, 30, 40, 50, 60, 70, 567, 89012345);
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        foreach ($data as $datum)
            $dw->append($datum);
        $dw->close();

        $dr = DataIO::openFile($dataFile);
        $readData = $dr->data();
        $dr->close();
        $this->assertEquals($data, $readData,
            sprintf("in: %s\nout: %s",
                json_encode($data), json_encode($readData)));
    }

    public function testDifferingSchemasWithPrimitives()
    {
        $dataFile = $this->addDataFile('data-prim.avr');

        $writer_schema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "verified", "type": "boolean", "default": "false"}
]}
JSON;
        $data = [
            ['username' => 'john', 'age' => 25, 'verified' => true],
            ['username' => 'ryan', 'age' => 23, 'verified' => false]
        ];
        $dw = DataIO::openFile($dataFile, 'w', $writer_schema);
        foreach ($data as $datum) {
            $dw->append($datum);
        }
        $dw->close();
        $reader_schema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"}
]}
JSON;
        $dr = DataIO::openFile($dataFile, 'r', $reader_schema);
        foreach ($dr->data() as $index => $record) {
            $this->assertEquals($data[$index]['username'], $record['username']);
        }
    }

    public function test_differing_schemas_with_complex_objects()
    {
        $dataFile = $this->addDataFile('data-complex.avr');

        $writersSchema = <<<JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON;

        $data = [
            [
                'username' => 'john',
                'something_fixed' => 'foo',
                'something_enum' => 'hello',
                'something_array' => [1,2,3],
                'something_map' => ['a' => 1, 'b' => 2],
                'something_record' => ['inner' => 2],
                'something_error' => ['code' => 403],
            ],
            [
                'username' => 'ryan',
                'something_fixed' => 'bar',
                'something_enum' => 'goodbye',
                'something_array' => [1,2,3],
                'something_map' => ['a' => 2, 'b' => 6],
                'something_record' => ['inner' => 1],
                'something_error' => ['code' => 401],
            ]
        ];
        $dw = DataIO::openFile($dataFile, 'w', $writersSchema);
        foreach ($data as $datum) {
            $dw->append($datum);
        }
        $dw->close();

        foreach (['fixed', 'enum', 'record', 'error', 'array' , 'map', 'union'] as $s) {
            $readersSchema = json_decode($writersSchema, true);
            $dr = DataIO::openFile($dataFile, 'r', json_encode($readersSchema));
            foreach ($dr->data() as $idx => $obj) {
                foreach ($readersSchema['fields'] as $field) {
                    $fieldName = $field['name'];
                    $this->assertEquals($data[$idx][$fieldName], $obj[$fieldName]);
                }
            }
            $dr->close();
        }
    }
}