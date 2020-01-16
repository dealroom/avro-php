<?php

namespace Avro\Datum;

use Avro\Avro;
use Avro\Exception\AvroException;
use Avro\Util\Gmp;
use Avro\IO\IOInterface;

class BinaryEncoder
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
     * @param null $datum actual value is ignored
     * @return null
     */
    public function writeNull($datum)
    {
        return null;
    }

    /**
     * @param boolean $datum
     * @return int
     */
    public function writeBoolean($datum)
    {
        $byte = $datum ? chr(1) : chr(0);
        return $this->write($byte);
    }

    /**
     * @param int $datum
     * @return int
     */
    public function writeInt($datum)
    {
        return $this->writeLong($datum);
    }

    /**
     * @param int $n
     * @return int
     */
    public function writeLong($n)
    {
        if (Avro::usesGmp()) {
            return $this->write(Gmp::encodeLong($n));
        } else {
            return $this->write(self::encodeLong($n));
        }
    }

    /**
     * @param float $datum
     * @return int
     */
    public function writeFloat($datum)
    {
        return $this->write(self::floatToIntBits($datum));
    }

    /**
     * @param float $datum
     * @return int
     */
    public function writeDouble($datum)
    {
        return $this->write(self::doubleToLongBits($datum));
    }

    /**
     * @param string $str
     * @return int
     */
    public function writeString($str)
    {
        return $this->writeBytes($str);
    }

    /**
     * @param string $bytes
     * @return int
     */
    public function writeBytes($bytes)
    {
        $this->writeLong(strlen($bytes));
        return $this->write($bytes);
    }

    /**
     * @param string $datum
     * @return int
     */
    public function write($datum)
    {
        return $this->io->write($datum);
    }

    /**
     * Performs encoding of the given float value to a binary string.
     * @param float $float
     * @return string bytes
     */
    public static function floatToIntBits($float)
    {
        return pack('f', (float) $float);
    }

    /**
     * Performs encoding of the given double value to a binary string.
     * @param double $double
     * @return string bytes
     */
    public static function doubleToLongBits($double)
    {
        return pack('d', (double) $double);
    }

    /**
     * @param int|string $n
     * @return string long $n encoded as bytes
     * @internal This relies on 64-bit PHP.
     */
    static public function encodeLong($n)
    {
        $n = (int) $n;
        $n = ($n << 1) ^ ($n >> 63);
        $str = '';
        while (0 != ($n & ~0x7F)) {
            $str .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }
        $str .= chr($n);
        return $str;
    }
}