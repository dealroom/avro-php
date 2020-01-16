<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class ArraySchema extends AbstractSchema
{
    /**
     * Named schema name or Schema of array element.
     * @var Name|SchemaInterface
     */
    private $items;

    /**
     * If the items schema.
     * @var boolean
     * @todo: couldn't we derive this from whether or not $this->items is an Name or an SchemaInterface?
     */
    private $isItemsSchemaFromSchemata = false;

    /**
     * @param string|mixed $items AvroNamedSchema name or object form of decoded JSON schema representation.
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct($items, $defaultNamespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(AbstractSchema::ARRAY_SCHEMA);

        $items_schema = null;
        if (
            is_string($items)
            && $items_schema = $schemata->schemaByName(new Name($items, null, $defaultNamespace))
        ) {
            $this->isItemsSchemaFromSchemata = true;
        } else {
            $items_schema = AbstractSchema::subparse($items, $defaultNamespace, $schemata);
        }

        $this->items = $items_schema;
    }


    /**
     * Named schema name or AvroSchema of this array schema's elements.
     * @return Name|SchemaInterface
     */
    public function getItems()
    {
        return $this->items;
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AbstractSchema::ITEMS_ATTR] = $this->isItemsSchemaFromSchemata
            ? $this->items->getQualifiedName()
            : $this->items->toAvro();

        return $avro;
    }
}