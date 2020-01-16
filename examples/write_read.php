#!/usr/bin/env php

<?php

require_once 'vendor/autoload.php';

// Write and read a data file

$writersSchemaJson = '{"name":"member","type":"record","fields":[{"name":"member_id", "type":"int"},{"name":"member_name", "type":"string"}]}';

$jose = ['member_id' => 1392, 'member_name' => 'Jose'];
$maria = ['member_id' => 1642, 'member_name' => 'Maria'];
$data = [$jose, $maria];

$file = 'data.avr';
// Open $file for writing, using the given writer's schema
$dataWriter = \Avro\DataFile\DataIO::openFile($file, 'w', $writersSchemaJson);

// Write each datum to the file
foreach ($data as $datum) {
    $dataWriter->append($datum);
}
// Tidy up
$dataWriter->close();

// Open $file (by default for reading) using the writer's schema
// included in the file
$dataReader = \Avro\DataFile\DataIO::openFile($file);
echo "from file:\n";
// Read each datum
foreach ($dataReader->data() as $datum) {
    echo var_export($datum, true) . "\n";
}
$dataReader->close();

// Create a data string
// Create a string io object.
$io = new \Avro\IO\StringIO();
// Create a datum writer object
$writersSchema = \Avro\Schema\AbstractSchema::parse($writersSchemaJson);
$writer = new \Avro\Datum\Writer($writersSchema);
$dataWriter = new \Avro\DataFile\DataIOWriter($io, $writer, $writersSchema);
foreach ($data as $datum) {
    $dataWriter->append($datum);
}
$dataWriter->close();

$binaryString = $io->__toString();

// Load the string data string
$readIO = new \Avro\IO\StringIO($binaryString);
$dataReader = new \Avro\DataFile\DataIOReader($readIO, new \Avro\Datum\Reader());
echo "from binary string:\n";
foreach ($dataReader->data() as $datum) {
    echo var_export($datum, true) . "\n";
}

/** Output
from file:
array (
  'member_id' => 1392,
  'member_name' => 'Jose',
)
array (
  'member_id' => 1642,
  'member_name' => 'Maria',
)
from binary string:
array (
  'member_id' => 1392,
  'member_name' => 'Jose',
)
array (
  'member_id' => 1642,
  'member_name' => 'Maria',
)
*/
