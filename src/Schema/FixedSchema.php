<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class FixedSchema extends NamedSchema
{
    /**
     * Byte count of this fixed schema data value.
     * @var int
     */
    private $size;

    /**
     * @param Name $name
     * @param string $doc Set to null, as fixed schemas don't have doc strings
     * @param int $size byte count of this fixed schema data value
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct($name, $doc, $size, &$schemata = null)
    {
        $doc = null; // Fixed schemas don't have doc strings.

        if (!is_integer($size)) {
            throw new SchemaParseException('Fixed Schema requires a valid integer for "size" attribute');
        }

        parent::__construct(AbstractSchema::FIXED_SCHEMA, $name, $doc, $schemata);

        return $this->size = $size;
    }

    /**
     * @return int
     */
    public function getSize()
    {
        return $this->size;
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AbstractSchema::SIZE_ATTR] = $this->size;

        return $avro;
    }
}