<?php

namespace Avro\Exception;

use Avro\Schema\SchemaInterface;

class IOSchemaMatchException extends AvroException
{
    /**
     * @param SchemaInterface $writersSchema
     * @param SchemaInterface $readersSchema
     */
    function __construct($writersSchema, $readersSchema)
    {
        parent::__construct(
            sprintf(
                "Writer's schema %s and Reader's schema %s do not match.",
                $writersSchema,
                $readersSchema
            )
        );
    }
}