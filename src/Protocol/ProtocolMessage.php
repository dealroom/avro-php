<?php

namespace Avro\Protocol;

use Avro\Schema\RecordSchema;
use Avro\Schema\SchemaInterface;

class ProtocolMessage
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var RecordSchema
     */
    private $request;

    /**
     * @var SchemaInterface|null
     */
    private $response;

    /**
     * ProtocolMessage constructor.
     * @param $name
     * @param RecordSchema $request
     * @param SchemaInterface $response
     */
    public function __construct($name, RecordSchema $request, SchemaInterface $response = null)
    {
        $this->name = $name;
        $this->request = $request;
        $this->response = $response;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return RecordSchema
     */
    public function getRequest()
    {
        return $this->request;
    }

    /**
     * @return SchemaInterface|null
     */
    public function getResponse()
    {
        return $this->response;
    }
}