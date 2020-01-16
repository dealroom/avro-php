<?php

namespace Avro\Tests\Traits;

trait DataFilesTrait
{
    private $dataFiles = [];

    protected function getBaseDir(): string
    {
        return dirname(dirname(__FILE__));
    }

    protected function getTmpDir(): string
    {
        return $this->getBaseDir() . DIRECTORY_SEPARATOR . 'tmp';
    }

    protected function getDataDir(): string
    {
        return $this->getBaseDir() . DIRECTORY_SEPARATOR . 'data';
    }

    protected function setupTmpDir()
    {
        if (!file_exists($this->getTmpDir())) {
            mkdir($this->getTmpDir());
        }
    }

    protected function addDataFile($dataFile): string
    {
        $path = $this->getTmpDir() . DIRECTORY_SEPARATOR . $dataFile . '.' . strftime("%Y%m%dT%H%M%S");
        $this->dataFiles[] = $path;
        return $path;
    }

    protected static function removeDataFile($dataFile): void
    {
        if (file_exists($dataFile)) {
            unlink($dataFile);
        }
    }

    protected function removeDataFiles(): void
    {
        foreach ($this->dataFiles as $dataFile) {
            $this->removeDataFile($dataFile);
        }
    }
}