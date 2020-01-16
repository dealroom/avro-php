<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class UnionSchema extends AbstractSchema
{
    /**
     * List of schemas of this union.
     * @var SchemaInterface[]
     */
    private $schemas;

    /**
     * List of indices of named schemas which are defined in $schemata.
     * @var int[]
     */
    public $schemaFromSchemataIndices = [];

    /**
     * @param SchemaInterface[] $schemas list of schemas in the union
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct(array $schemas, $defaultNamespace, NamedSchemata &$schemata = null)
    {
        parent::__construct(AbstractSchema::UNION_SCHEMA);

        $schemaTypes = array();
        foreach ($schemas as $index => $schema) {
            $isSchemaFromSchemata = false;
            $newSchema = null;
            if (
                is_string($schema)
                && $newSchema = $schemata->schemaByName(new Name($schema, null, $defaultNamespace))
            ) {
                $isSchemaFromSchemata = true;
            } else {
                $newSchema = self::subparse($schema, $defaultNamespace, $schemata);
            }

            $schemaType = $newSchema->type;
            if (
                self::isValidType($schemaType)
                && !self::isNamedType($schemaType)
                && in_array($schemaType, $schemaTypes)
            ) {
                throw new SchemaParseException(sprintf('"%s" is already in union', $schemaType));
            } else if (AbstractSchema::UNION_SCHEMA == $schemaType) {
                throw new SchemaParseException('Unions cannot contain other unions');
            } else {
                $schemaTypes[] = $schemaType;
                $this->schemas[] = $newSchema;
                if ($isSchemaFromSchemata) {
                    $this->schemaFromSchemataIndices[] = $index;
                }
            }
        }

    }

    /**
     * @return SchemaInterface[]
     */
    public function getSchemas()
    {
        return $this->schemas;
    }

    /**
     * @param $index
     * @return SchemaInterface the particular schema from the union for the given (zero-based) index.
     * @throws SchemaParseException if the index is invalid for this schema.
     */
    public function getSchemaByIndex($index)
    {
        if (count($this->schemas) > $index) {
            return $this->schemas[$index];
        }

        throw new SchemaParseException('Invalid union schema index');
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = array();

        foreach ($this->schemas as $index => $schema) {
            $avro[] = in_array($index, $this->schemaFromSchemataIndices)
                ? $schema->getQualifiedName()
                : $schema->toAvro();
        }

        return $avro;
    }
}