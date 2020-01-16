<?php

namespace Avro\Schema;

use Avro\Exception\SchemaParseException;

class Name
{
    /**
     * Character used to separate names comprising the fullname.
     * @var string
     */
    const NAME_SEPARATOR = '.';

    /**
     * Regular expression to validate name values.
     * @var string
     */
    const NAME_REGEXP = '/^[A-Za-z_][A-Za-z0-9_]*$/';

    /**
     * Valid names are matched by self::NAME_REGEXP.
     * @var string
     */
    private $name;

    /**
     * @var string
     */
    private $namespace;

    /**
     * @var string
     */
    private $fullname;

    /**
     * Name qualified as necessary given its default namespace.
     * @var string
     */
    private $qualifiedName;

    /**
     * Name constructor.
     * @param string $name
     * @param string $namespace
     * @param string $defaultNamespace
     * @throws SchemaParseException
     */
    public function __construct($name, $namespace, $defaultNamespace)
    {
        if (!is_string($name) || empty($name)) {
            throw new SchemaParseException('Name must be a non-empty string.');
        }

        if (strpos($name, self::NAME_SEPARATOR) && self::checkNamespaceNames($name)) {
            $this->fullname = $name;
        } else if (!preg_match(self::NAME_REGEXP, $name)) {
            throw new SchemaParseException(sprintf('Invalid name "%s"', $name));
        } else if (!is_null($namespace)) {
            $this->fullname = self::parseFullname($name, $namespace);
        } else if (!is_null($defaultNamespace)) {
            $this->fullname = self::parseFullname($name, $defaultNamespace);
        } else {
            $this->fullname = $name;
        }

        list($this->name, $this->namespace) = self::extractNamespace($this->fullname);

        $this->qualifiedName = is_null($this->namespace) || $this->namespace === $defaultNamespace
            ? $this->name
            : $this->fullname;
    }

    /**
     * @return array [$name, $namespace]
     */
    public function getNameAndNamespace()
    {
        return [$this->name, $this->namespace];
    }

    /**
     * @return string
     */
    public function getFullname()
    {
        return $this->fullname;
    }

    /**
     * @return string
     */
    public function getQualifiedName()
    {
        return $this->qualifiedName;
    }

    /**
     * @param $name
     * @param null $namespace
     * @return string[] [$name, $namespace]
     */
    public static function extractNamespace($name, $namespace = null)
    {
        $parts = explode(self::NAME_SEPARATOR, $name);
        if (count($parts) > 1) {
            $name = array_pop($parts);
            $namespace = join(self::NAME_SEPARATOR, $parts);
        }
        return [$name, $namespace];
    }

    /**
     * @param $name
     * @return bool
     */
    public static function isWellFormedName($name)
    {
        return is_string($name) && !empty($name) && preg_match(self::NAME_REGEXP, $name);
    }

    /**
     * @param string $namespace
     * @return bool
     * @throws SchemaParseException
     */
    private static function checkNamespaceNames($namespace)
    {
        foreach (explode(self::NAME_SEPARATOR, $namespace) as $n) {
            if (empty($n) || !preg_match(self::NAME_REGEXP, $n)) {
                throw new SchemaParseException(sprintf('Invalid name "%s"', $n));
            }
        }
        return true;
    }

    /**
     * @param string $name
     * @param string $namespace
     * @return string
     * @throws SchemaParseException
     */
    private static function parseFullname($name, $namespace)
    {
        if (!is_string($namespace) || empty($namespace)) {
            throw new SchemaParseException('Namespace must be a non-empty string.');
        }

        self::checkNamespaceNames($namespace);

        return $namespace . '.' . $name;
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->getFullname();
    }

}