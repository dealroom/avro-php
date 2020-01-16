<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class Field extends AbstractSchema
{

    /**
     * Fields name attribute name.
     * @var string
     */
    const FIELD_NAME_ATTR = 'name';

    /**
     * @var string
     */
    const DEFAULT_ATTR = 'default';

    /**
     * @var string
     */
    const ORDER_ATTR = 'order';

    /**
     * @var string
     */
    const ASC_SORT_ORDER = 'ascending';

    /**
     * @var string
     */
    const DESC_SORT_ORDER = 'descending';

    /**
     * @var string
     */
    const IGNORE_SORT_ORDER = 'ignore';

    /**
     * List of valid field sort order values.
     * @var array
     */
    private static $valid_field_sort_orders = [
        self::ASC_SORT_ORDER,
        self::DESC_SORT_ORDER,
        self::IGNORE_SORT_ORDER,
    ];

    /**
     * @var string
     */
    private $name;

    /**
     * @var bool
     */
    private $hasDefault;

    /**
     * Field default value.
     * @var string
     */
    private $default;

    /**
     * Sort order of this field.
     * @var string
     */
    private $order;

    /**
     * Whether or not the NamedSchema of this field is defined in the NamedSchemata instance.
     * @var bool
     */
    private $isTypeFromSchemata;

    /**
     * Documentation of this field.
     * @var string
     */
    private $doc;

    /**
     * @param string $name
     * @param SchemaInterface $schema
     * @param boolean $isTypeFromSchemata
     * @param $hasDefault
     * @param string $default
     * @param string $order
     * @param $doc
     * @throws SchemaParseException
     * @todo Check validity of $default value
     * @todo Check validity of $order value
     */
    public function __construct(
        $name,
        SchemaInterface $schema,
        $isTypeFromSchemata,
        $hasDefault,
        $default,
        $order = null,
        $doc = null
    )
    {
        if (!Name::isWellFormedName($name)) {
            throw new SchemaParseException('Field requires a "name" attribute');
        }

        $this->type = $schema;
        $this->isTypeFromSchemata = $isTypeFromSchemata;
        $this->name = $name;
        $this->hasDefault = $hasDefault;
        if ($this->hasDefault) {
            $this->default = $default;
        }
        $this->checkOrderValue($order);
        $this->order = $order;
        $this->doc = $doc;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return mixed
     */
    public function getDefaultValue()
    {
        return $this->default;
    }

    /**
     * @return bool
     */
    public function hasDefaultValue()
    {
        return $this->hasDefault;
    }

    /**
     * @return string
     */
    public function getDoc()
    {
        return $this->doc;
    }

    /**
     * @param string $order
     * @return bool
     */
    private static function isValidFieldSortOrder($order)
    {
        return in_array($order, self::$valid_field_sort_orders);
    }

    /**
     * @param string $order
     * @throws SchemaParseException
     */
    private static function checkOrderValue($order)
    {
        if (!is_null($order) && !self::isValidFieldSortOrder($order)) {
            throw new SchemaParseException(sprintf('Invalid field sort order %s', $order));
        }
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = [self::FIELD_NAME_ATTR => $this->name];

        $avro[AbstractSchema::TYPE_ATTR] = $this->isTypeFromSchemata
            ? $this->type->getQualifiedName()
            : $this->type->toAvro();

        if ($this->hasDefault) {
            $avro[self::DEFAULT_ATTR] = $this->default;
        }

        if ($this->order) {
            $avro[self::ORDER_ATTR] = $this->order;
        }

        if ($this->doc) {
            $avro[AbstractSchema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }
}