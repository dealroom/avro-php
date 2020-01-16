<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class NamedSchemata
{
    /**
     * @var NamedSchema[]
     */
    private $schemata;

    /**
     * NamesSchemata constructor.
     * @param array $schemata
     */
    public function __construct(array $schemata = [])
    {
        $this->schemata = $schemata;
    }

    public function listSchemas()
    {
        var_export($this->schemata);
        foreach($this->schemata as $sch) {
            echo 'Schema '.$sch->__toString()."\n";
        }
    }

    /**
     * @param string $fullname
     * @return bool
     */
    public function hasName($fullname)
    {
        return array_key_exists($fullname, $this->schemata);
    }

    /**
     * @param string $fullname
     * @return SchemaInterface|null
     */
    public function schema($fullname)
    {
        return isset($this->schemata[$fullname]) ? $this->schemata[$fullname] : null;
    }

    /**
     * @param Name $name
     * @return SchemaInterface|null
     */
    public function schemaByName(Name $name)
    {
        return $this->schema($name->getFullname());
    }

    /**
     * Creates a new NamedSchemata instance of this schemata instance
     * with the given $schema appended.
     * @param NamedSchema $schema to add to this existing schemata
     * @return NamedSchemata
     * @throws SchemaParseException
     */
    public function cloneWithNewSchema(NamedSchema $schema)
    {
        $name = $schema->getFullname();

        if (AbstractSchema::isValidType($name)) {
            throw new SchemaParseException(sprintf('Name "%s" is a reserved type name', $name));
        } else if ($this->hasName($name)) {
            throw new SchemaParseException(sprintf('Name "%s" is already in use', $name));
        }

        $schemata = new NamedSchemata($this->schemata);
        $schemata->schemata[$name] = $schema;

        return $schemata;
    }
}