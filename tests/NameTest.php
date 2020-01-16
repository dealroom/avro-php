<?php

namespace Avro\Tests;

use Avro\Exception\SchemaParseException;
use Avro\Schema\Name;
use PHPUnit\Framework\TestCase;

class NameTest extends TestCase
{
    /**
     * @dataProvider fullnameProvider
     */
    public function testFullname($name, $namespace, $defaultNamespace, $isValid, $expectedFullname)
    {
        try {
            $name = new Name($name, $namespace, $defaultNamespace);
            $this->assertTrue($isValid);
            $this->assertEquals($expectedFullname, $name->getFullname());
        } catch (SchemaParseException $e) {
            $this->assertFalse(
                $isValid, sprintf("%s:\n%s", $name, $e->getMessage())
            );
        }
    }

    /**
     * @dataProvider nameProvider
     */
    public function testName($name, $isWellFormed)
    {
        $this->assertEquals(Name::isWellFormedName($name), $isWellFormed);
    }

    public function fullnameProvider()
    {
        return [
            ['foo', null, null, true, 'foo'],
            ['foo', 'bar', null, true, 'bar.foo'],
            ['bar.foo', 'baz', null, true, 'bar.foo'],
            ['_bar.foo', 'baz', null, true, '_bar.foo'],
            ['bar._foo', 'baz', null, true, 'bar._foo'],
            ['3bar.foo', 'baz', null, false, null],
            ['bar.3foo', 'baz', null, false, null],
            ['b4r.foo', 'baz', null, true, 'b4r.foo'],
            ['bar.f0o', 'baz', null, true, 'bar.f0o'],
            [' .foo', 'baz', null, false, null],
            ['bar. foo', 'baz', null, false, null],
            ['bar. ', 'baz', null, false, null],
        ];
    }

    /**
     * @return array
     */
    public function nameProvider()
    {
        return [
            ['a', true],
            ['_', true],
            ['1a', false],
            ['', false],
            [null, false],
            [' ', false],
            ['Cons', true],
        ];
    }
}