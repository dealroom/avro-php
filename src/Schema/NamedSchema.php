<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class NamedSchema extends AbstractSchema
{
    /**
     * @var Name $name
     */
    private $name;

    /**
     * Documentation string.
     * @var string
     */
    private $doc;

    /**
     * NamedSchema constructor.
     * @param string $type
     * @param Name $name
     * @param string $doc documentation string
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct($type, Name $name, $doc = null, NamedSchemata &$schemata = null)
    {
        parent::__construct($type);

        $this->name = $name;

        if ($doc && !is_string($doc)) {
            throw new SchemaParseException('Schema doc attribute must be a string');
        }

        $this->doc = $doc;

        if (!is_null($schemata)) {
            $schemata = $schemata->cloneWithNewSchema($this);
        }
    }

    /**
     * @return string
     */
    public function getFullname()
    {
        return $this->name->getFullname();
    }

    /**
     * @return string
     */
    public function getQualifiedName()
    {
        return $this->name->getQualifiedName();
    }

    /**
     * @return string
     */
    public function getDoc()
    {
        return $this->doc;
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        list($name, $namespace) = Name::extractNamespace($this->getQualifiedName());

        $avro[AbstractSchema::NAME_ATTR] = $name;

        if ($namespace) {
            $avro[AbstractSchema::NAMESPACE_ATTR] = $namespace;
        }
        if (!is_null($this->doc)) {
            $avro[AbstractSchema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }
}