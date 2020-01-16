<?php

namespace Avro\Datum;

use Avro\Exception\AvroException;
use Avro\Exception\IOSchemaMatchException;
use Avro\Schema\AbstractSchema;
use Avro\Schema\ArraySchema;
use Avro\Schema\EnumSchema;
use Avro\Schema\FixedSchema;
use Avro\Schema\MapSchema;
use Avro\Schema\RecordSchema;
use Avro\Schema\SchemaInterface;
use Avro\Schema\UnionSchema;

class Reader
{
    /**
     * @var SchemaInterface
     */
    private $writersSchema;

    /**
     * @var SchemaInterface
     */
    private $readersSchema;

    /**
     * Reader constructor.
     * @param SchemaInterface $writersSchema
     * @param SchemaInterface $readersSchema
     */
    public function __construct(SchemaInterface $writersSchema = null, SchemaInterface $readersSchema = null)
    {
        $this->writersSchema = $writersSchema;
        $this->readersSchema = $readersSchema;
    }

    /**
     * @param SchemaInterface $writersSchema
     */
    public function setWritersSchema($writersSchema)
    {
        $this->writersSchema = $writersSchema;
    }

    /**
     * @param BinaryDecoder $decoder
     * @return string
     * @throws AvroException
     * @throws IOSchemaMatchException
     */
    public function read($decoder)
    {
        if (is_null($this->readersSchema)) {
            $this->readersSchema = $this->writersSchema;
        }

        return $this->readData($this->writersSchema, $this->readersSchema, $decoder);
    }

    /**
     * @param SchemaInterface $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return mixed
     * @throws AvroException
     * @throws IOSchemaMatchException
     */
    public function readData(SchemaInterface $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        if (!self::schemasMatch($writersSchema, $readersSchema)) {
            throw new IOSchemaMatchException($writersSchema, $readersSchema);
        }

        // Schema resolution: reader's schema is a union, writer's schema is not
        if (
            $readersSchema->getType() === AbstractSchema::UNION_SCHEMA
            && $writersSchema->getType() !== AbstractSchema::UNION_SCHEMA
        ) {
            foreach ($readersSchema->getSchemas() as $schema) {
                if (self::schemasMatch($writersSchema, $schema)) {
                    return $this->readData($writersSchema, $schema, $decoder);
                }
            }
            throw new IOSchemaMatchException($writersSchema, $readersSchema);
        }

        switch ($writersSchema->getType()) {
            case AbstractSchema::NULL_TYPE:
                return $decoder->readNull();
            case AbstractSchema::BOOLEAN_TYPE:
                return $decoder->readBoolean();
            case AbstractSchema::INT_TYPE:
                return $decoder->readInt();
            case AbstractSchema::LONG_TYPE:
                return $decoder->readLong();
            case AbstractSchema::FLOAT_TYPE:
                return $decoder->readFloat();
            case AbstractSchema::DOUBLE_TYPE:
                return $decoder->readDouble();
            case AbstractSchema::STRING_TYPE:
                return $decoder->readString();
            case AbstractSchema::BYTES_TYPE:
                return $decoder->readBytes();
            case AbstractSchema::ARRAY_SCHEMA:
                return $this->readArray($writersSchema, $readersSchema, $decoder);
            case AbstractSchema::MAP_SCHEMA:
                return $this->readMap($writersSchema, $readersSchema, $decoder);
            case AbstractSchema::UNION_SCHEMA:
                return $this->readUnion($writersSchema, $readersSchema, $decoder);
            case AbstractSchema::ENUM_SCHEMA:
                return $this->readEnum($writersSchema, $readersSchema, $decoder);
            case AbstractSchema::FIXED_SCHEMA:
                return $this->readFixed($writersSchema, $readersSchema, $decoder);
            case AbstractSchema::RECORD_SCHEMA:
            case AbstractSchema::ERROR_SCHEMA:
            case AbstractSchema::REQUEST_SCHEMA:
                return $this->readRecord($writersSchema, $readersSchema, $decoder);
            default:
                throw new AvroException(
                    sprintf("Cannot read unknown schema type: %s", $writersSchema->getType())
                );
        }
    }

    /**
     * @param ArraySchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return array
     * @throws AvroException
     * @throws IOSchemaMatchException
     */
    public function readArray(ArraySchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        $items = [];
        $blockCount = $decoder->readLong();
        while ($blockCount !== 0) {
            if ($blockCount < 0) {
                $blockCount = -$blockCount;
                $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $blockCount; $i++) {
                $items []= $this->readData($writersSchema->getItems(), $readersSchema->getItems(), $decoder);
            }
            $blockCount = $decoder->readLong();
        }
        return $items;
    }

    /**
     * @param MapSchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return array
     * @throws AvroException
     * @throws IOSchemaMatchException
     */
    public function readMap(MapSchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        $items = [];
        $pairCount = $decoder->readLong();
        while ($pairCount !== 0) {
            if ($pairCount < 0) {
                $pairCount = -$pairCount;
                // Note: Ingoring what we read here
                $decoder->readLong();
            }

            for ($i = 0; $i < $pairCount; $i++) {
                $key = $decoder->readString();
                $items[$key] = $this->readData($writersSchema->getValues(), $readersSchema->getValues(), $decoder);
            }
            $pairCount = $decoder->readLong();
        }
        return $items;
    }

    /**
     * @param UnionSchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return mixed
     * @throws AvroException
     * @throws IOSchemaMatchException
     */
    public function readUnion(UnionSchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        $schemaIndex = $decoder->readLong();
        $selectedWritersSchema = $writersSchema->getSchemaByIndex($schemaIndex);
        return $this->readData($selectedWritersSchema, $readersSchema, $decoder);
    }

    /**
     * @param EnumSchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return string
     * @throws AvroException
     */
    public function readEnum(EnumSchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        $symbolIndex = $decoder->readInt();
        $symbol = $writersSchema->getSymbolByIndex($symbolIndex);
        if (!$readersSchema->hasSymbol($symbol)) {
            null;
        } // FIXME: unset wrt schema resolution
        return $symbol;
    }

    /**
     * @param FixedSchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return string
     */
    public function readFixed(FixedSchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        return $decoder->read(
            $writersSchema->getSize()
        );
    }

    /**
     * @param RecordSchema $writersSchema
     * @param SchemaInterface $readersSchema
     * @param BinaryDecoder $decoder
     * @return array
     * @throws AvroException
     */
    public function readRecord(RecordSchema $writersSchema, SchemaInterface $readersSchema, BinaryDecoder $decoder)
    {
        $readersFields = $readersSchema->getFieldsHash();
        $record = [];
        foreach ($writersSchema->getFields() as $writersField) {
            $type = $writersField->getType();
            if (isset($readersFields[$writersField->getName()])) {
                $record[$writersField->getName()] = $this->readData(
                    $type,
                    $readersFields[$writersField->getName()]->getType(),
                    $decoder
                );
            } else {
                $this->skipData($type, $decoder);
            }
        }

        // Fill in default values
        if (count($readersFields) > count($record)) {
            $writersFields = $writersSchema->getFieldsHash();
            foreach ($readersFields as $fieldName => $field) {
                if (!isset($writersFields[$fieldName])) {
                    if ($field->hasDefaultValue()) {
                        $record[$field->getName()] = $this->readDefaultValue(
                            $field->getType(),
                            $field->getDefaultValue()
                        );
                    } else {
                        null; // FIXME: unset
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param SchemaInterface $fieldSchema
     * @param null|boolean|int|float|string|array $defaultValue
     * @return null|boolean|int|float|string|array
     * @throws AvroException
     */
    public function readDefaultValue(SchemaInterface $fieldSchema, $defaultValue)
    {
        switch($fieldSchema->getType()) {
            case AbstractSchema::NULL_TYPE:
                return null;
            case AbstractSchema::BOOLEAN_TYPE:
            case AbstractSchema::STRING_TYPE:
            case AbstractSchema::BYTES_TYPE:
            case AbstractSchema::ENUM_SCHEMA:
            case AbstractSchema::FIXED_SCHEMA:
                return $defaultValue;
            case AbstractSchema::INT_TYPE:
            case AbstractSchema::LONG_TYPE:
                return (int) $defaultValue;
            case AbstractSchema::FLOAT_TYPE:
            case AbstractSchema::DOUBLE_TYPE:
                return (float) $defaultValue;
            case AbstractSchema::ARRAY_SCHEMA:
                $array = array();
                foreach ($defaultValue as $json_val) {
                    $val = $this->readDefaultValue($fieldSchema->getItems(), $json_val);
                    $array[] = $val;
                }
                return $array;
            case AbstractSchema::MAP_SCHEMA:
                $map = array();
                foreach ($defaultValue as $key => $json_val) {
                    $map[$key] = $this->readDefaultValue($fieldSchema->getValues(), $json_val);
                }
                return $map;
            case AbstractSchema::UNION_SCHEMA:
                return $this->readDefaultValue($fieldSchema->getSchemaByIndex(0), $defaultValue);
            case AbstractSchema::RECORD_SCHEMA:
                $record = [];
                foreach ($fieldSchema->getFields() as $field) {
                    $fieldName = $field->getName();
                    if (!$jsonVal = $defaultValue[$fieldName]) {
                        $jsonVal = $field->getDefaultValue();
                    }
                    $record[$fieldName] = $this->readDefaultValue($field->getType(), $jsonVal);
                }
                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $fieldSchema->getType()));
        }
    }

    /**
     * @param SchemaInterface $writersSchema
     * @param BinaryDecoder $decoder
     * @return mixed
     * @throws AvroException
     */
    private function skipData(SchemaInterface $writersSchema, BinaryDecoder $decoder)
    {
        switch ($writersSchema->getType()) {
            case AbstractSchema::NULL_TYPE:
                return $decoder->skipNull();
            case AbstractSchema::BOOLEAN_TYPE:
                return $decoder->skipBoolean();
            case AbstractSchema::INT_TYPE:
                return $decoder->skipInt();
            case AbstractSchema::LONG_TYPE:
                return $decoder->skipLong();
            case AbstractSchema::FLOAT_TYPE:
                return $decoder->skipFloat();
            case AbstractSchema::DOUBLE_TYPE:
                return $decoder->skipDouble();
            case AbstractSchema::STRING_TYPE:
                return $decoder->skipString();
            case AbstractSchema::BYTES_TYPE:
                return $decoder->skipBytes();
            case AbstractSchema::ARRAY_SCHEMA:
                return $decoder->skipArray($writersSchema, $decoder);
            case AbstractSchema::MAP_SCHEMA:
                return $decoder->skipMap($writersSchema, $decoder);
            case AbstractSchema::UNION_SCHEMA:
                return $decoder->skipUnion($writersSchema, $decoder);
            case AbstractSchema::ENUM_SCHEMA:
                return $decoder->skipEnum($writersSchema, $decoder);
            case AbstractSchema::FIXED_SCHEMA:
                return $decoder->skipFixed($writersSchema, $decoder);
            case AbstractSchema::RECORD_SCHEMA:
            case AbstractSchema::ERROR_SCHEMA:
            case AbstractSchema::REQUEST_SCHEMA:
                return $decoder->skipRecord($writersSchema, $decoder);
            default:
                throw new AvroException(sprintf('Uknown schema type: %s', $writersSchema->getType()));
        }
    }

    /**
     *
     * @param SchemaInterface $writersSchema
     * @param SchemaInterface $readersSchema
     * @return bool
     */
    static function schemasMatch(SchemaInterface $writersSchema, SchemaInterface $readersSchema)
    {
        $writersSchemaType = $writersSchema->getType();
        $readersSchemaType = $readersSchema->getType();

        if (
            AbstractSchema::UNION_SCHEMA === $writersSchemaType
            || AbstractSchema::UNION_SCHEMA === $readersSchemaType
        ) {
            return true;
        }

        if ($writersSchemaType === $readersSchemaType) {
            if (AbstractSchema::isPrimitiveType($writersSchemaType)) {
                return true;
            }

            switch ($readersSchemaType) {
                case AbstractSchema::MAP_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->getValues(),
                        $readersSchema->getValues(),
                        [AbstractSchema::TYPE_ATTR]
                    );
                case AbstractSchema::ARRAY_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->getItems(),
                        $readersSchema->getItems(),
                        [AbstractSchema::TYPE_ATTR]
                    );
                case AbstractSchema::ENUM_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [AbstractSchema::FULLNAME_ATTR]
                    );
                case AbstractSchema::FIXED_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [AbstractSchema::FULLNAME_ATTR, AbstractSchema::SIZE_ATTR]
                    );
                case AbstractSchema::RECORD_SCHEMA:
                case AbstractSchema::ERROR_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [AbstractSchema::FULLNAME_ATTR]
                    );
                case AbstractSchema::REQUEST_SCHEMA:
                    // XXX: This seems wrong
                    return true;
                // XXX: no default
            }

            if (
                AbstractSchema::INT_TYPE === $writersSchema
                && in_array($readersSchemaType, [
                    AbstractSchema::LONG_TYPE,
                    AbstractSchema::FLOAT_TYPE,
                    AbstractSchema::DOUBLE_TYPE
                ])
            ) {
                return true;
            }

            if (
                AbstractSchema::LONG_TYPE === $writersSchema
                && in_array($readersSchemaType, [
                    AbstractSchema::FLOAT_TYPE,
                    AbstractSchema::DOUBLE_TYPE
                ])
            ) {
                return true;
            }

            if (
                AbstractSchema::FLOAT_TYPE === $writersSchema
                && AbstractSchema::DOUBLE_TYPE === $readersSchemaType
            ) {
                return true;
            }

            return false;
        }

    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param SchemaInterface $schemaOne
     * @param SchemaInterface $schemaTwo
     * @param string[] $attributeNames array of string attribute names to compare
     * @return bool
     */
    public static function attributesMatch(SchemaInterface $schemaOne, SchemaInterface $schemaTwo, array $attributeNames)
    {
        foreach ($attributeNames as $attributeName) {
            if ($schemaOne->attribute($attributeName) !== $schemaTwo->attribute($attributeName)) {
                return false;
            }
        }
        return true;
    }
}