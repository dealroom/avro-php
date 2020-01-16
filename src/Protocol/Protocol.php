<?php

namespace Avro\Protocol;

use Avro\Schema\NamedSchemata;
use Avro\Schema\SchemaInterface;

class Protocol
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var string
     */
    private $namespace;

    /**
     * @var NamedSchemata
     */
    private $schemata;

    /**
     * @var SchemaInterface
     */
    private $types;

    /**
     * @var array
     */
    private $messages = [];

    /**
     * Protocol constructor.
     * @param $name
     * @param $namespace
     * @param NamedSchemata $schemata
     * @param SchemaInterface|null $types
     * @param array $messages
     */
    public function __construct($name, $namespace, NamedSchemata $schemata, SchemaInterface $types = null, array $messages = [])
    {
        $this->name = $name;
        $this->namespace = $namespace;
        $this->schemata = $schemata;
        $this->types = $types;
        $this->messages = $messages;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getNamespace()
    {
        return $this->namespace;
    }

    /**
     * @return NamedSchemata
     */
    public function getSchemata()
    {
        return $this->schemata;
    }

    /**
     * @return SchemaInterface|null
     */
    public function getTypes()
    {
        return $this->types;
    }

    /**
     * @return array
     */
    public function getMessages()
    {
        return $this->messages;
    }
}