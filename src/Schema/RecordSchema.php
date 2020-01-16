<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Helpers;

class RecordSchema extends NamedSchema
{

    /**
     * Array of NamedSchema field definitions of this RecordSchema.
     * @var Field[]
     */
    private $fields = [];

    /**
     * Map of field names to field objects.
     * Not called directly. Memoization of RecordSchema->fieldsHash()
     * @var array
     */
    private $fieldsHash = [];

    /**
     * @param Name $name
     * @param string $doc
     * @param array $fields
     * @param NamedSchemata &$schemata
     * @param string $schema_type schema type name
     * @throws SchemaParseException
     */
    public function __construct(
        Name $name,
        $doc,
        $fields,
        NamedSchemata &$schemata = null,
        $schema_type = AbstractSchema::RECORD_SCHEMA
    )
    {
        if (is_null($fields)) {
            throw new SchemaParseException('Record schema requires a non-empty fields attribute');
        }

        if (AbstractSchema::REQUEST_SCHEMA === $schema_type) {
            parent::__construct($schema_type, $name);
        } else {
            parent::__construct($schema_type, $name, $doc, $schemata);
        }

        list(, $namespace) = $name->getNameAndNamespace();
        $this->fields = self::parseFields($fields, $namespace, $schemata);
    }

    /**
     * @return Field[]
     */
    public function getFields()
    {
        return $this->fields;
    }

    /**
     * @return Field[]
     */
    public function getFieldsHash()
    {
        if (!$this->fieldsHash) {
            $hash = array();
            foreach ($this->fields as $field) {
                $hash[$field->getName()] = $field;
            }
            $this->fieldsHash = $hash;
        }

        return $this->fieldsHash;
    }

    /**
     * @param mixed $fieldData
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @return Field[]
     * @throws SchemaParseException
     */
    static function parseFields($fieldData, $defaultNamespace, &$schemata)
    {
        $fields = array();
        $fieldNames = array();
        foreach ($fieldData as $index => $field) {
            $name = Helpers::arrayValue($field, Field::FIELD_NAME_ATTR);
            $type = Helpers::arrayValue($field, AbstractSchema::TYPE_ATTR);
            $order = Helpers::arrayValue($field, Field::ORDER_ATTR);
            $doc = Helpers::arrayValue($field, AbstractSchema::DOC_ATTR);

            $default = null;
            $hasDefault = false;
            if (array_key_exists(Field::DEFAULT_ATTR, $field)) {
                $default = $field[Field::DEFAULT_ATTR];
                $hasDefault = true;
            }

            if (in_array($name, $fieldNames)) {
                throw new SchemaParseException(sprintf("Field name %s is already in use", $name));
            }

            $isSchemaFromSchemata = false;
            $fieldSchema = null;
            if (
                is_string($type)
                && $fieldSchema = $schemata->schemaByName(new Name($type, null, $defaultNamespace))
            ) {
                $isSchemaFromSchemata = true;
            } else {
                $fieldSchema = self::subparse($type, $defaultNamespace, $schemata);
            }

            $newField = new Field($name, $fieldSchema, $isSchemaFromSchemata, $hasDefault, $default, $order, $doc);
            $fieldNames[] = $name;
            $fields[] = $newField;
        }

        return $fields;
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $fields_avro = [];
        foreach ($this->fields as $field) {
            $fields_avro[] = $field->toAvro();
        }

        if (AbstractSchema::REQUEST_SCHEMA === $this->type) {
            return $fields_avro;
        }

        $avro[AbstractSchema::FIELDS_ATTR] = $fields_avro;

        return $avro;
    }
}