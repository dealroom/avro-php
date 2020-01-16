<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\Exception\AvroException;
use Avro\Util\Gmp;
use Avro\IO\IOInterface;

class BinaryDecoder
{
    /**
     * @var IOInterface
     */
    private $io;

    /**
     * @param IOInterface $io
     * @throws AvroException
     */
    public function __construct(IOInterface $io)
    {
        Avro::checkPlatform();

        $this->io = $io;
    }

    /**
     * @return string
     */
    private function nextByte()
    {
        return $this->read(1);
    }

    /**
     * @return null
     */
    public function readNull()
    {
        return null;
    }

    /**
     * @return boolean
     */
    public function readBoolean()
    {
        return (boolean) (1 == ord($this->nextByte()));
    }

    /**
     * @return int
     */
    public function readInt()
    {
        return (int) $this->readLong();
    }

    /**
     * @return int
     */
    public function readLong()
    {
        $byte = ord($this->nextByte());
        $bytes = array($byte);
        while (($byte & 0x80) !== 0) {
            $byte = ord($this->nextByte());
            $bytes[] = $byte;
        }

        if (Avro::usesGmp())
            return Gmp::decodeLongFromArray($bytes);

        return self::decodeLongFromArray($bytes);
    }

    /**
     * @return float
     */
    public function readFloat()
    {
        return self::intBitsToFloat($this->read(4));
    }

    /**
     * @return double
     */
    public function readDouble()
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * A string is encoded as a long followed by that many bytes of UTF-8 encoded character data.
     * @return string
     */
    public function readString()
    {
        return $this->readBytes();
    }

    /**
     * @return string
     */
    public function readBytes()
    {
        return $this->read($this->readLong());
    }

    /**
     * @param int $len count of bytes to read
     * @return string
     */
    public function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @return null
     */
    public function skipNull()
    {
        return null;
    }

    public function skipBoolean()
    {
        return $this->skip(1);
    }

    public function skipInt()
    {
        return $this->skipLong();
    }

    public function skipLong()
    {
        $b = ord($this->nextByte());
        while (($b & 0x80) !== 0) {
            $b = ord($this->nextByte());
        }
    }

    public function skipFloat()
    {
        return $this->skip(4);
    }

    public function skipDouble()
    {
        return $this->skip(8);
    }

    public function skipBytes()
    {
        return $this->skip($this->readLong());
    }

    public function skipString()
    {
        return $this->skipBytes();
    }

    /**
     * @param int $len count of bytes to skip
     */
    public function skip($len)
    {
        $this->seek($len, IOInterface::SEEK_CUR);
    }

    /**
     * @return int position of pointer in AvroIO instance
     */
    private function tell()
    {
        return $this->io->tell();
    }

    /**
     * @param int $offset
     * @param int $whence
     * @return bool
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @param int[] array of byte ascii values
     * @return int decoded value
     * @internal Requires 64-bit platform
     */
    public static function decodeLongFromArray($bytes)
    {
        $b = array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80))
        {
            $b = array_shift($bytes);
            $n |= (($b & 0x7f) << $shift);
            $shift += 7;
        }
        return (($n >> 1) ^ -($n & 1));
    }

    /**
     * Performs decoding of the binary string to a float value.
     * @param string $bits
     * @return float
     */
    static public function intBitsToFloat($bits)
    {
        $float = unpack('f', $bits);
        return (float) $float[1];
    }

    /**
     * Performs decoding of the binary string to a double value.
     * @param string $bits
     * @return float
     */
    static public function longBitsToDouble($bits)
    {
        $double = unpack('d', $bits);
        return (double) $double[1];
    }
}