<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class MapSchema extends AbstractSchema
{
    /**
     * Named schema name or SchemaInterface of map schema values.
     * @var Name|SchemaInterface
     */
    private $values;

    /**
     * @var bool
     * @todo Couldn't we derive this based on whether or not $this->values is a string?
     */
    private $isValuesSchemaFromSchemata = false;

    /**
     * @param string|SchemaInterface $values
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct($values, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AbstractSchema::MAP_SCHEMA);

        $values_schema = null;
        if (
            is_string($values)
            && $values_schema = $schemata->schemaByName(new Name($values, null, $defaultNamespace))
        ) {
            $this->isValuesSchemaFromSchemata = true;
        } else {
            $values_schema = AbstractSchema::subparse($values, $defaultNamespace, $schemata);
        }

        $this->values = $values_schema;
    }

    /**
     * @return SchemaInterface
     */
    public function getValues()
    {
        return $this->values;
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AbstractSchema::VALUES_ATTR] = $this->isValuesSchemaFromSchemata
            ? $this->values->getQualifiedName()
            : $this->values->toAvro();

        return $avro;
    }
}