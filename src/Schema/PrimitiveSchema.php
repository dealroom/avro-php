<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class PrimitiveSchema extends AbstractSchema
{
    /**
     * @param string $type the primitive schema type name
     * @throws SchemaParseException
     */
    public function __construct($type)
    {
        if (!self::isPrimitiveType($type)) {
            throw new SchemaParseException(sprintf('%s is not a valid primitive type.', $type));
        }

        parent::__construct($type);
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        if (count($avro) === 1) {
            return $this->type;
        }

        return $avro;
    }
}