<?php

namespace Avro\IO;

use Avro\Exception\IOException;

class FileIO implements IOInterface
{
    /**
     * Fopen read mode value. Used internally.
     * @var string
     */
    const FOPEN_READ_MODE = 'rb';

    /**
     * Fopen write mode value. Used internally.
     * @var string
     */
    const FOPEN_WRITE_MODE = 'wb';

    /**
     * @var string
     */
    private $filePath;

    /**
     * @var resource
     */
    private $fileHandle;

    /**
     * FileIO constructor.
     * @param $filePath
     * @param string $mode
     * @throws IOException
     */
    public function __construct($filePath, $mode = self::READ_MODE)
    {
        if ($mode === self::READ_MODE && !file_exists($filePath)) {
            throw new IOException('File not found');
        }

        $this->filePath = $filePath;

        $this->handleFile($mode);
    }

    /**
     * @param $mode
     * @throws IOException
     */
    private function handleFile($mode)
    {
        switch ($mode) {
            case self::WRITE_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_WRITE_MODE);
                if ($this->fileHandle === false) {
                    throw new IOException('Could not open file for writing');
                }
                break;
            case self::READ_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_READ_MODE);
                if ($this->fileHandle === false) {
                    throw new IOException('Could not open file for reading');
                }
                break;
            default:
                throw new IOException(
                    sprintf(
                        "Only modes '%s' and '%s' allowed. You provided '%s'.",
                        self::READ_MODE,
                        self::WRITE_MODE, $mode
                    )
                );
        }
    }

    /**
     * @param string $str
     * @return int count of bytes written
     * @throws IOException
     */
    public function write($str)
    {
        $len = fwrite($this->fileHandle, $str);

        if ($len === false) {
            throw new IOException(sprintf('Could not write to file'));
        }

        return $len;
    }

    /**
     * @param int $len count of bytes to read.
     * @return string bytes read
     * @throws IOException
     */
    public function read($len)
    {
        if ($len <= 0) {
            throw new IOException(sprintf("Invalid length value passed to read: %d", $len));
        }

        if ($len === 0) {
            return '';
        }

        $bytes = fread($this->fileHandle, $len);

        if ($bytes === false) {
            throw new IOException('Could not read from file');
        }

        return $bytes;
    }

    /**
     * @return int current position within the file
     * @throws IOException
     */
    public function tell()
    {
        $position = ftell($this->fileHandle);

        if ($position === false) {
            throw new IOException('Could not execute tell on reader');
        }

        return $position;
    }

    /**
     * @param int $offset
     * @param int $whence
     * @return bool
     * @throws IOException
     */
    public function seek($offset, $whence = self::SEEK_SET)
    {
        $res = fseek($this->fileHandle, $offset, $whence);

        // Note: does not catch seeking beyond end of file
        if ($res === -1) {
            throw new IOException(sprintf("Could not execute seek (offset = %d, whence = %d)", $offset, $whence));
        }

        return true;
    }

    /**
     * Closes the file.
     * @return bool
     * @throws IOException
     */
    public function close()
    {
        $res = fclose($this->fileHandle);

        if ($res === false) {
            throw new IOException('Error closing file.');
        }

        return $res;
    }

    /**
     * @return bool
     * @throws IOException
     */
    public function isEof()
    {
        $this->read(1);

        if (feof($this->fileHandle)) {
            return true;
        }

        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    /**
     * @return bool
     * @throws IOException
     */
    public function flush()
    {
        $res = fflush($this->fileHandle);

        if ($res === false) {
            throw new IOException('Could not flush file.');
        }

        return true;
    }
}