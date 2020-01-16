<?php

namespace Avro\IO;

use Avro\Exception\IOException;

class StringIO implements IOInterface
{
    /**
     * @var string
     */
    private $stringBuffer = '';

    /**
     * Current position in string.
     * @var int
     */
    private $currentIndex = 0;

    /**
     * Whether or not the string is closed.
     * @var bool
     */
    private $isClosed = false;

    /**
     * StringIO constructor.
     * @param string $str initial value of AvroStringIO buffer. Regardless
     *                    of the initial value, the pointer is set to the
     *                    beginning of the buffer.
     * @throws IOException
     */
    public function __construct($str = '')
    {
        if (!is_string($str)) {
            throw new IOException(sprintf('constructor argument must be a string: %s', gettype($str)));
        }

        $this->stringBuffer .= $str;
    }

    /**
     * Append bytes to this buffer.
     * (Nothing more is needed to support Avro.)
     * @param string $data bytes to write
     * @return int count of bytes written.
     * @throws IOException if $args is not a string value.
     */
    public function write($data)
    {
        $this->checkClosed();

        if (!is_string($data)) {
            throw new IOException(
                sprintf('write argument must be a string: (%s) %s', gettype($data), var_export($data, true))
            );
        }

        return $this->appendStr($data);

    }

    /**
     * @param int $len
     * @return string bytes read from buffer
     * @throws IOException
     * @todo test for fencepost errors wrt updating current_index
     */
    public function read($len)
    {
        $this->checkClosed();

        $read = '';
        for ($i = $this->currentIndex; $i < $this->currentIndex + $len; $i++) {
            $read .= $this->stringBuffer[$i];
        }
        if (strlen($read) < $len) {
            $this->currentIndex = $this->getLength();
        } else {
            $this->currentIndex += $len;
        }

        return $read;
    }

    /**
     * @param int $offset
     * @param int $whence
     * @return bool
     * @throws IOException
     */
    public function seek($offset, $whence = self::SEEK_SET)
    {
        if (!is_int($offset)) {
            throw new IOException('Seek offset must be an integer.');
        }

        // Prevent seeking before BOF
        switch ($whence) {
            case self::SEEK_SET:
                if (0 > $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $offset;
                break;
            case self::SEEK_CUR:
                if (0 > $this->currentIndex + $whence) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex += $offset;
                break;
            case self::SEEK_END:
                if (0 > $this->getLength() + $offset) {
                    throw new IOException('Cannot seek before beginning of file.');
                }
                $this->currentIndex = $this->getLength() + $offset;
                break;
            default:
                throw new IOException(sprintf('Invalid seek whence %d', $whence));
        }

        return true;
    }

    /**
     * @return int
     */
    public function tell()
    {
        return $this->currentIndex;
    }

    /**
     * @return bool
     */
    public function isEof()
    {
        return $this->currentIndex >= $this->getLength();
    }

    /**
     * @return bool
     */
    public function flush()
    {
        return true;
    }

    /**
     * Marks this buffer as closed.
     * @return bool
     * @throws IOException
     */
    public function close()
    {
        $this->checkClosed();
        $this->isClosed = true;
        return true;
    }

    /**
     * @throws IOException
     */
    private function checkClosed()
    {
        if ($this->isClosed()) {
            throw new IOException('Buffer is closed');
        }
    }

    /**
     * @return bool
     */
    public function isClosed()
    {
        return $this->isClosed;
    }

    /**
     * Appends bytes to this buffer.
     * @param string $str
     * @return int count of bytes written.
     * @throws IOException
     */
    private function appendStr($str)
    {
        $this->checkClosed();
        $this->stringBuffer .= $str;
        $len = strlen($str);
        $this->currentIndex += $len;
        return $len;
    }

    /**
     * Truncates the truncate buffer to 0 bytes and returns the pointer
     * to the beginning of the buffer.
     * @return bool
     * @throws IOException
     */
    public function truncate()
    {
        $this->checkClosed();
        $this->stringBuffer = '';
        $this->currentIndex = 0;
        return true;
    }

    /**
     * Get count of bytes in the buffer.
     * @return int
     */
    public function getLength()
    {
        return strlen($this->stringBuffer);
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return $this->stringBuffer;
    }
}