<?php

namespace Avro\Schema;

use Avro\Exception\AvroException;
use Avro\Exception\SchemaParseException;
use Avro\Util\Helpers;

class EnumSchema extends NamedSchema
{
    /**
     * @var string[]
     */
    private $symbols;

    /**
     * @param Name $name
     * @param string $doc
     * @param string[] $symbols
     * @param NamedSchemata &$schemata
     * @throws SchemaParseException
     */
    public function __construct($name, $doc, $symbols, &$schemata = null)
    {
        if (!Helpers::isList($symbols)) {
            throw new SchemaParseException('Enum Schema symbols are not a list');
        }

        if (count(array_unique($symbols)) > count($symbols)) {
            throw new SchemaParseException(sprintf('Duplicate symbols: %s', $symbols));
        }

        foreach ($symbols as $symbol) {
            if (!is_string($symbol) || empty($symbol)) {
                throw new SchemaParseException(
                    sprintf('Enum schema symbol must be a string %', print_r($symbol, true))
                );
            }
        }

        parent::__construct(AbstractSchema::ENUM_SCHEMA, $name, $doc, $schemata);

        $this->symbols = $symbols;
    }

    /**
     * @return string[]
     */
    public function getSymbols()
    {
        return $this->symbols;
    }

    /**
     * @param string $symbol
     * @return bool
     */
    public function hasSymbol($symbol)
    {
        return in_array($symbol, $this->symbols);
    }

    /**
     * @param int $index
     * @return string enum schema symbol with the given (zero-based) index
     * @throws AvroException
     */
    public function getSymbolByIndex($index)
    {
        if (!array_key_exists($index, $this->symbols)) {
            throw new AvroException(sprintf('Invalid symbol index %d', $index));
        }

        return $this->symbols[$index];
    }

    /**
     * @param string $symbol
     * @return int the index of the given $symbol in the enum schema
     * @throws AvroException
     */
    public function getSymbolIndex($symbol)
    {
        if (($idx = array_search($symbol, $this->symbols, true)) === false) {
            throw new AvroException(sprintf("Invalid symbol value '%s'", $symbol));
        }

        return $idx;
    }

    /**
     * @return array
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AbstractSchema::SYMBOLS_ATTR] = $this->symbols;

        return $avro;
    }
}