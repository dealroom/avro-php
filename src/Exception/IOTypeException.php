<?php

namespace Avro\Exception;

use Avro\Schema\SchemaInterface;

class IOTypeException extends AvroException
{
    /**
     * @param SchemaInterface $expectedSchema
     * @param mixed $datum
     */
    public function __construct($expectedSchema, $datum)
    {
        parent::__construct(
            sprintf(
                'The datum %s is not an example of schema %s',
                var_export($datum, true),
                $expectedSchema
            )
        );
    }
}