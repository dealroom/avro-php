<?php

namespace Avro\Datum;

use Avro\Exception\AvroException;
use Avro\Exception\IOTypeException;
use Avro\Exception\SchemaParseException;
use Avro\Schema\AbstractSchema;
use Avro\Schema\ArraySchema;
use Avro\Schema\EnumSchema;
use Avro\Schema\MapSchema;
use Avro\Schema\RecordSchema;
use Avro\Schema\SchemaInterface;
use Avro\Schema\UnionSchema;

class Writer
{
    /**
     * Schema used by this instance to write Avro data.
     * @var SchemaInterface
     */
    private $writersSchema;

    /**
     * @param SchemaInterface $writersSchema
     */
    public function __construct(SchemaInterface $writersSchema = null)
    {
        $this->writersSchema = $writersSchema;
    }

    public function setWritersSchema(SchemaInterface $writersSchema)
    {
        $this->writersSchema = $writersSchema;
    }

    public function getWritersSchema()
    {
        return $this->writersSchema;
    }

    /**
     * @param $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     * @throws IOTypeException
     */
    public function writeData($writersSchema, $datum, BinaryEncoder $encoder)
    {
        if (!AbstractSchema::isValidDatum($writersSchema, $datum)) {
            throw new IOTypeException($writersSchema, $datum);
        }

        switch ($writersSchema->getType()) {
            case AbstractSchema::NULL_TYPE:
                $encoder->writeNull($datum);
                break;
            case AbstractSchema::BOOLEAN_TYPE:
                $encoder->writeBoolean($datum);
                break;
            case AbstractSchema::INT_TYPE:
                $encoder->writeInt($datum);
                break;
            case AbstractSchema::LONG_TYPE:
                $encoder->writeLong($datum);
                break;
            case AbstractSchema::FLOAT_TYPE:
                $encoder->writeFloat($datum);
                break;
            case AbstractSchema::DOUBLE_TYPE:
                $encoder->writeDouble($datum);
                break;
            case AbstractSchema::STRING_TYPE:
                $encoder->writeString($datum);
                break;
            case AbstractSchema::BYTES_TYPE:
                $encoder->writeBytes($datum);
                break;
            case AbstractSchema::ARRAY_SCHEMA:
                $this->writeArray($writersSchema, $datum, $encoder);
                break;
            case AbstractSchema::MAP_SCHEMA:
                $this->writeMap($writersSchema, $datum, $encoder);
                break;
            case AbstractSchema::FIXED_SCHEMA:
                $this->writeFixed($writersSchema, $datum, $encoder);
                break;
            case AbstractSchema::ENUM_SCHEMA:
                $this->writeEnum($writersSchema, $datum, $encoder);
                break;
            case AbstractSchema::RECORD_SCHEMA:
            case AbstractSchema::ERROR_SCHEMA:
            case AbstractSchema::REQUEST_SCHEMA:
                $this->writeRecord($writersSchema, $datum, $encoder);
            break;
            case AbstractSchema::UNION_SCHEMA:
                $this->writeUnion($writersSchema, $datum, $encoder);
                break;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $writersSchema->getType()));
        }
    }

    /**
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     */
    public function write($datum, BinaryEncoder $encoder)
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }

    /**
     * @param ArraySchema $writersSchema
     * @param null|boolean|int|float|string|array $datum item to be written
     * @param BinaryEncoder $encoder
     * @throws AvroException
     */
    private function writeArray(ArraySchema $writersSchema, $datum, BinaryEncoder $encoder)
    {
        $datumCount = count($datum);

        if ($datumCount > 0) {
            $encoder->writeLong($datumCount);
            $items = $writersSchema->getItems();
            foreach ($datum as $item) {
                $this->writeData($items, $item, $encoder);
            }
        }

        $encoder->writeLong(0);
    }

    /**
     * @param MapSchema $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     * @throws IOTypeException
     */
    private function writeMap(MapSchema $writersSchema, $datum, BinaryEncoder $encoder)
    {
        $datumCount = count($datum);

        if ($datumCount > 0) {
            $encoder->writeLong($datumCount);
            foreach ($datum as $key => $value) {
                $encoder->writeString($key);
                $this->writeData($writersSchema->getValues(), $value, $encoder);
            }
        }

        $encoder->writeLong(0);
    }

    /**
     * @param UnionSchema $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     * @throws IOTypeException
     * @throws SchemaParseException
     */
    private function writeUnion(UnionSchema $writersSchema, $datum, BinaryEncoder $encoder)
    {
        $datumSchemaIndex = -1;
        $datumSchema = null;
        foreach ($writersSchema->getSchemas() as $index => $schema) {
            if (AbstractSchema::isValidDatum($schema, $datum)) {
                $datumSchemaIndex = $index;
                $datumSchema = $schema;
                break;
            }
        }

        if (is_null($datumSchema)) {
            throw new IOTypeException($writersSchema, $datum);
        }

        $encoder->writeLong($datumSchemaIndex);
        $this->writeData($datumSchema, $datum, $encoder);
    }

    /**
     * @param EnumSchema $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     */
    private function writeEnum(EnumSchema $writersSchema, $datum, BinaryEncoder $encoder)
    {
        $encoder->writeInt(
            $writersSchema->getSymbolIndex($datum)
        );
    }

    /**
     * @param SchemaInterface $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     */
    private function writeFixed(SchemaInterface $writersSchema, $datum, BinaryEncoder $encoder)
    {
        $encoder->write($datum);
    }

    /**
     * @param RecordSchema $writersSchema
     * @param $datum
     * @param BinaryEncoder $encoder
     * @throws AvroException
     * @throws IOTypeException
     */
    private function writeRecord(RecordSchema $writersSchema, $datum, BinaryEncoder $encoder)
    {
        foreach ($writersSchema->getFields() as $field) {
            $this->writeData($field->getType(), $datum[$field->getName()], $encoder);
        }
    }
}