<?php

namespace Avro\Tests;

use Avro\DataFile\DataIOReader;
use Avro\DataFile\DataIOWriter;
use Avro\Datum\Reader;
use Avro\Datum\Writer;
use Avro\IO\StringIO;
use Avro\Schema\AbstractSchema;
use Avro\Util\Debug;
use PHPUnit\Framework\TestCase;

class StringIOTest extends TestCase
{
    public function testWrite()
    {
        $strio = new StringIO();
        $this->assertEquals(0, $strio->tell());
        $str = 'foo';
        $strlen = strlen($str);
        $this->assertEquals($strlen, $strio->write($str));
        $this->assertEquals($strlen, $strio->tell());
    }

    public function testSeek()
    {
        $this->markTestIncomplete('This test has not been implemented yet.');
    }

    public function test_tell()
    {
        $this->markTestIncomplete('This test has not been implemented yet.');
    }

    public function test_read()
    {
        $this->markTestIncomplete('This test has not been implemented yet.');
    }

    public function testStringRep()
    {
        $writersSchemaJson = '"null"';
        $writersSchema = AbstractSchema::parse($writersSchemaJson);
        $datumWriter = new Writer($writersSchema);
        $strio = new StringIO();
        $this->assertEquals('', $strio->__toString());
        $dw = new DataIOWriter($strio, $datumWriter, $writersSchemaJson);
        $dw->close();

        $this->assertEquals(57, strlen($strio->__toString()), Debug::asciiString($strio->__toString()));

        $readStrio = new StringIO($strio->__toString());

        $datumReader = new Reader();
        $dr = new DataIOReader($readStrio, $datumReader);
        $readData = $dr->data();
        $datumCount = count($readData);
        $this->assertEquals(0, $datumCount);
    }
}