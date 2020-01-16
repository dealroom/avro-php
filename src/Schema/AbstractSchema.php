<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;
use Avro\Util\Helpers;

abstract class AbstractSchema implements SchemaInterface
{
    /**
     * Lower bound of integer values: -(1 << 31).
     * @var int
     */
    const INT_MIN_VALUE = -2147483648;

    /**
     * Upper bound of integer values: (1 << 31) - 1.
     * @var int
     */
    const INT_MAX_VALUE = 2147483647;

    /**
     * Lower bound of long values: -(1 << 63).
     * @var int
     */
    const LONG_MIN_VALUE = -9223372036854775808;

    /**
     * Upper bound of long values: (1 << 63) - 1.
     * @var int
     */
    const LONG_MAX_VALUE = 9223372036854775807;

    /**
     * Null schema type name.
     * @var string
     */
    const NULL_TYPE = 'null';

    /**
     * Boolean schema type name.
     * @var string
     */
    const BOOLEAN_TYPE = 'boolean';

    /**
     * Int schema type name.
     * Int schema type value is a 32-bit signed int.
     * @var string
     */
    const INT_TYPE = 'int';

    /**
     * Long schema type name.
     * Long schema type value is a 64-bit signed int.
     * @var string
     */
    const LONG_TYPE = 'long';

    /**
     * Float schema type name.
     * Float schema type value is a 32-bit IEEE 754 floating-point number.
     * @var string
     */
    const FLOAT_TYPE = 'float';

    /**
     * Double schema type name.
     * Double schema type value is a 64-bit IEEE 754 floating-point number.
     * @var string
     */
    const DOUBLE_TYPE = 'double';

    /**
     * String schema type name.
     * String schema type value is a Unicode character sequence.
     * @var string
     */
    const STRING_TYPE = 'string';

    /**
     * Bytes schema type name.
     * Bytes schema type value is a sequence of 8-bit unsigned bytes.
     * @var string
     */
    const BYTES_TYPE = 'bytes';

    /**
     * Array schema type name.
     * @var string
     */
    const ARRAY_SCHEMA = 'array';

    /**
     * Map schema type name.
     * @var string
     */
    const MAP_SCHEMA = 'map';

    /**
     * Union schema type name.
     * @var string
     */
    const UNION_SCHEMA = 'union';

    /**
     * error_union schema type name.
     * Unions of error schemas are used by Avro messages.
     * @var string
     */
    const ERROR_UNION_SCHEMA = 'error_union';

    /**
     * Enum schema type name.
     * @var string
     */
    const ENUM_SCHEMA = 'enum';

    /**
     * Fixed schema type name.
     * @var string
     */
    const FIXED_SCHEMA = 'fixed';

    /**
     * Record schema type name.
     * @var string
     */
    const RECORD_SCHEMA = 'record';

    /**
     * Error schema type name.
     * @var string
     */
    const ERROR_SCHEMA = 'error';

    /**
     * Request schema type name.
     * @var string
     */
    const REQUEST_SCHEMA = 'request';

    /**
     * Schema type name attribute name.
     * @var string
     */
    const TYPE_ATTR = 'type';

    /**
     * Named schema name attribute name.
     * @var string
     */
    const NAME_ATTR = 'name';

    /**
     * Named schema namespace attribute name.
     * @var string
     */
    const NAMESPACE_ATTR = 'namespace';

    /**
     * Derived attribute: doesn't appear in schema
     * @var string
     */
    const FULLNAME_ATTR = 'fullname';

    /**
     * Array schema size attribute name.
     * @var string
     */
    const SIZE_ATTR = 'size';

    /**
     * Record fields attribute name.
     * @var string
     */
    const FIELDS_ATTR = 'fields';

    /**
     * Array schema items attribute name.
     * @var string
     */
    const ITEMS_ATTR = 'items';

    /**
     * Enum schema symbols attribute name.
     * @var string
     */
    const SYMBOLS_ATTR = 'symbols';

    /**
     * Map schema values attribute name.
     * @var string
     */
    const VALUES_ATTR = 'values';

    /**
     * Document string attribute name.
     * @var string
     */
    const DOC_ATTR = 'doc';

    /**
     * List of primitive schema type names.
     * @var array
     */
    private static $primitiveTypes = [
        self::NULL_TYPE,
        self::BOOLEAN_TYPE,
        self::STRING_TYPE,
        self::BYTES_TYPE,
        self::INT_TYPE,
        self::LONG_TYPE,
        self::FLOAT_TYPE,
        self::DOUBLE_TYPE,
    ];

    /**
     * List of named schema type names.
     * @var array
     */
    private static $namedTypes = [
        self::FIXED_SCHEMA,
        self::ENUM_SCHEMA,
        self::RECORD_SCHEMA,
        self::ERROR_SCHEMA,
    ];

    /**
     * List of names of reserved attributes.
     * @var array
     */
    private static $reservedAttrs = [
        self::TYPE_ATTR,
        self::NAME_ATTR,
        self::NAMESPACE_ATTR,
        self::FIELDS_ATTR,
        self::ITEMS_ATTR,
        self::SIZE_ATTR,
        self::SYMBOLS_ATTR,
        self::VALUES_ATTR
    ];

    /**
     * Schema type.
     * @var string
     */
    protected $type;

    /**
     * Schema constructor.
     * Should only be called from within the constructor of a class which extends AbstractSchema.
     * @param string $type a schema type name
     */
    public function __construct($type)
    {
        $this->type = $type;
    }

    /**
     * Get schema type name of this schema.
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @param $attribute
     * @return mixed
     */
    public function attribute($attribute)
    {
        // @todo maybe check and exception
        $method = sprintf('get%s', ucfirst($attribute));

        return $this->$method();
    }

    /**
     * @param string $type a schema type name
     * @return bool
     */
    public static function isNamedType($type)
    {
        return in_array($type, self::$namedTypes);
    }

    /**
     * @param string $type a schema type name
     * @return bool
     */
    public static function isPrimitiveType($type)
    {
        return in_array($type, self::$primitiveTypes);
    }

    /**
     * @param string $type a schema type name
     * @return bool
     */
    public static function isValidType($type)
    {
        return self::isPrimitiveType($type)
            || self::isNamedType($type)
            || in_array($type, [
                    self::ARRAY_SCHEMA,
                    self::MAP_SCHEMA,
                    self::UNION_SCHEMA,
                    self::REQUEST_SCHEMA,
                    self::ERROR_UNION_SCHEMA
                ]);
    }

    /**
     * @param string $json JSON-encoded schema
     * @return SchemaInterface
     * @throws SchemaParseException
     */
    public static function parse($json)
    {
        $schemata = new NamedSchemata();
        $avro = json_decode($json, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new SchemaParseException(
                "JSON decode error " . json_last_error() . ": " . json_last_error_msg()
            );
        }
        return self::realParse($avro, null, $schemata);
    }

    /**
     * @param mixed $avro JSON-decoded schema
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata reference to named schemas
     * @return SchemaInterface
     * @throws SchemaParseException
     */
    static function realParse($avro, $defaultNamespace = null, &$schemata = null)
    {
        if (is_null($schemata)) {
            $schemata = new NamedSchemata();
        }

        if (is_array($avro)) {
            $type = Helpers::arrayValue($avro, self::TYPE_ATTR);

            if (self::isPrimitiveType($type)) {
                return new PrimitiveSchema($type);
            } else if (self::isNamedType($type)) {
                $name = Helpers::arrayValue($avro, self::NAME_ATTR);
                $namespace = Helpers::arrayValue($avro, self::NAMESPACE_ATTR);
                $new_name = new Name($name, $namespace, $defaultNamespace);
                $doc = Helpers::arrayValue($avro, self::DOC_ATTR);
                switch ($type) {
                    case self::FIXED_SCHEMA:
                        $size = Helpers::arrayValue($avro, self::SIZE_ATTR);
                        return new FixedSchema($new_name, $doc, $size, $schemata);
                    case self::ENUM_SCHEMA:
                        $symbols = Helpers::arrayValue($avro, self::SYMBOLS_ATTR);
                        return new EnumSchema($new_name, $doc, $symbols, $schemata);
                    case self::RECORD_SCHEMA:
                    case self::ERROR_SCHEMA:
                        $fields = Helpers::arrayValue($avro, self::FIELDS_ATTR);
                        return new RecordSchema($new_name, $doc, $fields, $schemata, $type);
                    default:
                        throw new SchemaParseException(sprintf('Unknown named type: %s', $type));
                }
            } else if (self::isValidType($type)) {
                switch ($type) {
                    case self::ARRAY_SCHEMA:
                        return new ArraySchema($avro[self::ITEMS_ATTR], $defaultNamespace, $schemata);
                    case self::MAP_SCHEMA:
                        return new MapSchema($avro[self::VALUES_ATTR], $defaultNamespace, $schemata);
                    default:
                        throw new SchemaParseException(sprintf('Unknown valid type: %s', $type));
                }
            } else if (!array_key_exists(self::TYPE_ATTR, $avro) && Helpers::isList($avro)) {
                return new UnionSchema($avro, $defaultNamespace, $schemata);
            } else {
                throw new SchemaParseException(sprintf('Undefined type: %s', $type));
            }
        } else if (self::isPrimitiveType($avro)) {
            return new PrimitiveSchema($avro);
        } else {
            throw new SchemaParseException(
                sprintf('%s is not a schema we know about.', print_r($avro, true))
            );
        }
    }

    /**
     * @param $expectedSchema
     * @param $datum
     * @return bool
     * @throws SchemaParseException
     */
    public static function isValidDatum(SchemaInterface $expectedSchema, $datum)
    {
        switch ($expectedSchema->getType()) {
            case self::NULL_TYPE:
                return is_null($datum);
            case self::BOOLEAN_TYPE:
                return is_bool($datum);
            case self::STRING_TYPE:
            case self::BYTES_TYPE:
                return is_string($datum);
            case self::INT_TYPE:
                return is_int($datum) && self::INT_MIN_VALUE <= $datum && $datum <= self::INT_MAX_VALUE;
            case self::LONG_TYPE:
                return is_int($datum) && self::LONG_MIN_VALUE <= $datum && $datum <= self::LONG_MAX_VALUE;
            case self::FLOAT_TYPE:
            case self::DOUBLE_TYPE:
                return is_float($datum) || is_int($datum);
            case self::ARRAY_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $d) {
                        if (!self::isValidDatum($expectedSchema->getItems(), $d)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::MAP_SCHEMA:
                if (is_array($datum)) {
                    foreach ($datum as $key => $value) {
                        if (!is_string($key) || !self::isValidDatum($expectedSchema->getValues(), $value)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            case self::UNION_SCHEMA:
                foreach ($expectedSchema->getSchemas() as $schema) {
                    if (self::isValidDatum($schema, $datum)) {
                        return true;
                    }
                }
                return false;
            case self::ENUM_SCHEMA:
                return in_array($datum, $expectedSchema->getSymbols());
            case self::FIXED_SCHEMA:
                return is_string($datum) && strlen($datum) == $expectedSchema->getSize();
            case self::RECORD_SCHEMA:
            case self::ERROR_SCHEMA:
            case self::REQUEST_SCHEMA:
                if (is_array($datum)) {
                    foreach ($expectedSchema->getFields() as $field) {
                        if (
                            !array_key_exists($field->getName(), $datum)
                            || !self::isValidDatum($field->getType(), $datum[$field->getName()])
                        ) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            default:
                throw new SchemaParseException(sprintf('%s is not allowed.', $expectedSchema));
        }
    }

    /**
     * @param mixed $avro
     * @param string $defaultNamespace namespace of enclosing schema
     * @param NamedSchemata &$schemata
     * @return SchemaInterface
     * @throws SchemaParseException
     */
    protected static function subparse($avro, $defaultNamespace, &$schemata = null)
    {
        try {
            return self::realParse($avro, $defaultNamespace, $schemata);
        } catch (SchemaParseException $e) {
            throw $e;
        } catch (\Exception $e) {
            throw new SchemaParseException(
                sprintf('Sub-schema is not a valid Avro schema. Bad schema: %s', print_r($avro, true))
            );
        }

    }

    /**
     * @return array
     */
    public function toAvro()
    {
        return [self::TYPE_ATTR => $this->type];
    }

    /**
     * Converts into JSON-encoded representation of this Avro schema.
     * @return string
     */
    public function __toString()
    {
        return json_encode($this->toAvro());
    }
}