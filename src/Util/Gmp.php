<?php

namespace Avro\Util;

class Gmp {

    /**
     * Memoized GMP resource for zero.
     * @var resource
     */
    private static $gmp_0;

    /**
     * Memoized GMP resource for one (1).
     * @var resource
     */
    private static $gmp_1;

    /**
     * Memoized GMP resource for two (2).
     * @var resource
     */
    private static $gmp_2;

    /**
     * Memoized GMP resource for 0x7f.
     * @var resource
     */
    private static $gmp_0x7f;

    /**
     * Memoized GMP resource for 64-bit ~0x7f.
     * @var resource
     */
    private static $gmp_n0x7f;

    /**
     * Memoized GMP resource for 64-bits of 1.
     * @var resource
     */
    private static $gmp_0xfs;

    /**
     * GMP resource for zero.
     * @return resource
     */
    private static function gmp_0()
    {
        if (!isset(self::$gmp_0)) {
            self::$gmp_0 = gmp_init('0');
        }

        return self::$gmp_0;
    }

    /**
     * GMP resource for one (1).
     * @return resource
     */
    private static function gmp_1()
    {
        if (!isset(self::$gmp_1)) {
            self::$gmp_1 = gmp_init('1');
        }

        return self::$gmp_1;
    }

    /**
     * GMP resource for two (2).
     * @return resource
     */
    private static function gmp_2()
    {
        if (!isset(self::$gmp_2)) {
            self::$gmp_2 = gmp_init('2');
        }

        return self::$gmp_2;
    }

    /**
     * GMP resource for 0x7f.
     * @return resource
     */
    private static function gmp_0x7f()
    {
        if (!isset(self::$gmp_0x7f)) {
            self::$gmp_0x7f = gmp_init('0x7f');
        }

        return self::$gmp_0x7f;
    }

    /**
     * GMP resource for 64-bit ~0x7f.
     * @return resource
     */
    private static function gmp_n0x7f()
    {
        if (!isset(self::$gmp_n0x7f)) {
            self::$gmp_n0x7f = gmp_init('0xffffffffffffff80');
        }

        return self::$gmp_n0x7f;
    }

    /**
     * GMP resource for 64-bits of 1
     * @return resource
     */
    private static function gmp_0xfs()
    {
        if (!isset(self::$gmp_0xfs)) {
            self::$gmp_0xfs = gmp_init('0xffffffffffffffff');
        }

        return self::$gmp_0xfs;
    }

    /**
     * @param GMP resource
     * @return GMP resource 64-bit two's complement of input.
     */
    public static function gmpTwosComplement($g)
    {
        return gmp_neg(gmp_sub(gmp_pow(self::gmp_2(), 64), $g));
    }

    /**
     * @interal Only works up to shift 63 (doesn't wrap bits around).
     * @param resource|int|string $g
     * @param int $shift number of bits to shift left
     * @return resource $g shifted left
     */
    public static function shiftLeft($g, $shift)
    {
        if ($shift === 0) {
            return $g;
        }

        if (gmp_sign($g) < 0) {
            $g = self::gmpTwosComplement($g);
        }

        $m = gmp_mul($g, gmp_pow(self::gmp_2(), $shift));
        $m = gmp_and($m, self::gmp_0xfs());
        if (gmp_testbit($m, 63)) {
            $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp_0xfs()), self::gmp_1()));
        }

        return $m;
    }

    /**
     * Arithmetic right shift
     * @param resource|int|string $g
     * @param int $shift number of bits to shift right
     * @return resource $g shifted right $shift bits
     */
    public static function shift_right($g, $shift)
    {
        if ($shift === 0) {
            return $g;
        }

        if (gmp_sign($g) >= 0) {
            $m = gmp_div($g, gmp_pow(self::gmp_2(), $shift));
        } else {
            $g = gmp_and($g, self::gmp_0xfs());
            $m = gmp_div($g, gmp_pow(self::gmp_2(), $shift));
            $m = gmp_and($m, self::gmp_0xfs());
            for ($i = 63; $i >= (63 - $shift); $i--) {
                gmp_setbit($m, $i);
            }

            $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp_0xfs()), self::gmp_1()));
        }

        return $m;
    }

    /**
     * @param int|string $n integer (or string representation of integer) to encode
     * @return string $bytes of the long $n encoded per the Avro spec
     */
    public static function encodeLong($n)
    {
        $g = gmp_init($n);
        $g = gmp_xor(self::shiftLeft($g, 1), self::shiftRight($g, 63));
        $bytes = '';
        while (gmp_cmp(self::gmp_0(), gmp_and($g, self::gmp_n0x7f())) !== 0) {
            $bytes .= chr(gmp_intval(gmp_and($g, self::gmp_0x7f())) | 0x80);
            $g = self::shiftRight($g, 7);
        }
        $bytes .= chr(gmp_intval($g));

        return $bytes;
    }

    /**
     * @param int[] $bytes array of ascii codes of bytes to decode
     * @return string representation of decoded long.
     */
    public static function decodeLongFromArray($bytes)
    {
        $b = array_shift($bytes);
        $g = gmp_init($b & 0x7f);
        $shift = 7;
        while (($b & 0x80) !== 0) {
            $b = array_shift($bytes);
            $g = gmp_or($g, self::shiftLeft(($b & 0x7f), $shift));
            $shift += 7;
        }

        $val = gmp_xor(self::shiftRight($g, 1), gmp_neg(gmp_and($g, 1)));

        return gmp_strval($val);
    }

}