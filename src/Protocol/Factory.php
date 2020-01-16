<?php

namespace Avro\Protocol;

use Avro\Exception\ProtocolParseException;
use Avro\Schema\AbstractSchema;
use Avro\Schema\Name;
use Avro\Schema\NamedSchemata;
use Avro\Schema\PrimitiveSchema;
use Avro\Schema\RecordSchema;
use Avro\Exception\SchemaParseException;

class Factory
{
    /**
     * @param $json
     * @return Protocol
     * @throws ProtocolParseException
     * @throws SchemaParseException
     */
    public static function parseProtocol($json)
    {
        if (is_null($json)) {
            throw new ProtocolParseException( "Protocol can't be null");
        }

        $avro = json_decode($json, true);

        $name = $avro['protocol'];
        $namespace = $avro['namespace'];
        $schemata = new NamedSchemata();
        $messages = [];
        $types = !is_null($avro['types']) ? AbstractSchema::realParse($avro['types'], $namespace, $schemata) : null;

        if (!is_null($avro['messages'])) {
            foreach ($avro['messages'] as $messageName => $messageAvro) {
                $messages[$messageName] = self::parseProtocolMessage($messageName, $namespace, $messageAvro, $schemata);
            }
        }

        return new Protocol($name, $namespace, $schemata, $types, $messages);
    }

    /**
     * @param $name
     * @param $namespace
     * @param array $avro
     * @param NamedSchemata $schemata
     * @return ProtocolMessage
     * @throws SchemaParseException
     */
    public static function parseProtocolMessage($name, $namespace, array $avro, NamedSchemata $schemata)
    {
        $request = new RecordSchema(
            new Name($name, null, $namespace),
            null,
            $avro['request'],
            $schemata,
            AbstractSchema::REQUEST_SCHEMA
        );

        $response = null;
        if (array_key_exists('response', $avro)) {
            $response = $schemata->schemaByName(
                new Name($avro['response'], $namespace, $namespace)
            );
            if (is_null($response)) {
                $response = new PrimitiveSchema($avro['response']);
            }
        }

        return new ProtocolMessage($namespace, $request, $response);
    }
}