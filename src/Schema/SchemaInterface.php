<?php

namespace Avro\Schema;

interface SchemaInterface
{
    /**
     * @return string
     */
    public function getType();

    /**
     * @return mixed
     */
    public function toAvro();
}