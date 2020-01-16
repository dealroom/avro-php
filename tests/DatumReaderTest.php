<?php

namespace Avro\Tests;

use Avro\Datum\Reader;
use Avro\Schema\AbstractSchema;
use PHPUnit\Framework\TestCase;

class DatumReaderTest extends TestCase
{
    public function testSchemaMatching()
    {
        $writersSchema = '{ "type": "map", "values": "bytes" }';
        $readersSchema = $writersSchema;
        $this->assertTrue(
            Reader::schemasMatch(
                AbstractSchema::parse($writersSchema),
                AbstractSchema::parse($readersSchema)
            )
        );
    }
}