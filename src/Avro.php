<?php

namespace Avro;

use Avro\Exception\AvroException;
use Avro\Util\Debug;

class Avro
{
    /**
     * Version number of Avro specification to which this implementation complies
     * @var string
     */
    const SPEC_VERSION = '1.3.3';

    /**
     * Constant to enumerate big endian.
     * @access private
     * @var int
     */
    const BIG_ENDIAN = 0x00;

    /**
     * Constant to enumerate little endian.
     * @access private
     * @var int
     */
    const LITTLE_ENDIAN = 0x01;

    /**
     * Memoized result of self::setEndianness()
     * @var int self::BIG_ENDIAN or self::LITTLE_ENDIAN
     * @see self::setEndianness()
     */
    private static $endianness;

    /**
     * Constant to enumerate biginteger handling mode.
     * GMP is used, if available, on 32-bit platforms.
     */
    const PHP_BIGINTEGER_MODE = 0x00;
    const GMP_BIGINTEGER_MODE = 0x01;

    /**
     * Mode used to handle bigintegers. After Avro::check64Bit() has been called,
     * (usually via a call to Avro::checkPlatform(), set to
     * self::GMP_BIGINTEGER_MODE on 32-bit platforms that have GMP available,
     * and to self::PHP_BIGINTEGER_MODE otherwise.
     * @var int
     */
    private static $bigIntegerMode;

    /**
     * Wrapper method to call each required check.
     * @throws AvroException
     */
    public static function checkPlatform()
    {
        self::check64Bit();
        self::checkLittleEndian();
    }

    /**
     * Determines if the host platform can encode and decode long integer data.
     *
     * @throws AvroException if the platform cannot handle long integers.
     */
    private static function check64Bit()
    {
        if (8 != PHP_INT_SIZE) {
            if (extension_loaded('gmp')) {
                self::$bigIntegerMode = self::GMP_BIGINTEGER_MODE;
            } else {
                throw new AvroException(
                    'This platform cannot handle a 64-bit operations. Please install the GMP PHP extension.'
                );
            }
        } else {
            self::$bigIntegerMode = self::PHP_BIGINTEGER_MODE;
        }

    }

    /**
     * Requires Avro::check64Bit() (exposed via Avro::checkPlatform())
     * to have been called to set Avro::$bigintegerMode.
     * @return bool
     */
    public static function usesGmp()
    {
        return self::GMP_BIGINTEGER_MODE === self::$bigIntegerMode;
    }

    /**
     * Determines if the host platform is little endian,
     * required for processing double and float data.
     *
     * @throws AvroException if the platform is not little endian.
     */
    private static function checkLittleEndian()
    {
        if (!self::isLittleEndianPlatform()) {
            throw new AvroException('This is not a little-endian platform');
        }
    }

    /**
     * Determines the endianness of the host platform and memoizes
     * the result to Avro::$endianness.
     *
     * Based on a similar check performed in http://pear.php.net/package/Math_BinaryUtils
     *
     * @throws AvroException if the endianness cannot be determined.
     */
    private static function setEndianness()
    {
        $packed = pack('d', 1);
        switch ($packed) {
            case "\77\360\0\0\0\0\0\0":
                self::$endianness = self::BIG_ENDIAN;
                break;
            case "\0\0\0\0\0\0\360\77":
                self::$endianness = self::LITTLE_ENDIAN;
                break;
            default:
                throw new AvroException(
                    sprintf('Error determining platform endianness: %s', Debug::hexString($packed))
                );
        }
    }

    /**
     * Check if the host platform is big endian.
     * @return bool
     * @throws AvroException
     */
    private static function isBigEndianPlatform()
    {
        if (is_null(self::$endianness)) {
            self::setEndianness();
        }

        return self::BIG_ENDIAN === self::$endianness;
    }

    /**
     * Check if the host platform is little endian.
     * @return bool
     * @throws AvroException
     */
    private static function isLittleEndianPlatform()
    {
        return !self::isBigEndianPlatform();
    }
}