<?php

namespace Avro\Tests;

use Avro\DataFile\DataIO;
use Avro\Tests\Traits\DataFilesTrait;
use PHPUnit\Framework\TestCase;

class FileIOTest extends TestCase
{
    use DataFilesTrait;

    protected function tearDown(): void
    {
        $file = $this->getTmpFile();
        if (file_exists($file)) {
            unlink($file);
        }
    }

    private function getFileName(): string
    {
        return $this->getDataDir() . DIRECTORY_SEPARATOR . 'users.avro';
    }

    private function getTmpFile(): string
    {
        return $this->getTmpDir() . DIRECTORY_SEPARATOR . 'users.avro';
    }

    private function read()
    {
        $fileName = $this->getFileName();
        $reader = DataIO::openFile($fileName);
        return $reader->data();
    }

    public function testReading()
    {
        $expected = [
            [
                'name' => 'Alyssa',
                'favorite_color' => null,
                'favorite_numbers' => [3, 9, 15, 20],
            ],
            [
                'name' => 'Ben',
                'favorite_color' => 'red',
                'favorite_numbers' => [],
            ]
        ];
        $this->assertEquals($expected, $this->read());
    }
}