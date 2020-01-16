<?php

namespace Avro\Tests;

use Avro\Util\Debug;
use PHPUnit\Framework\TestCase;

class FloatIntEncodingTest extends TestCase
{
    const FLOAT_TYPE = 'float';
    const DOUBLE_TYPE = 'double';

    static $FLOAT_NAN;
    static $FLOAT_POS_INF;
    static $FLOAT_NEG_INF;
    static $DOUBLE_NAN;
    static $DOUBLE_POS_INF;
    static $DOUBLE_NEG_INF;

    static $LONG_BITS_NAN;
    static $LONG_BITS_POS_INF;
    static $LONG_BITS_NEG_INF;
    static $INT_BITS_NAN;
    static $INT_BITS_POS_INF;
    static $INT_BITS_NEG_INF;

    public function setUp():void
    {
        $this->makeSpecialVals();
    }

    protected function makeSpecialVals()
    {
        self::$DOUBLE_NAN = (double) NAN;
        self::$DOUBLE_POS_INF = (double) INF;
        self::$DOUBLE_NEG_INF = (double) -INF;
        self::$FLOAT_NAN = (float) NAN;
        self::$FLOAT_POS_INF = (float) INF;
        self::$FLOAT_NEG_INF = (float) -INF;

        self::$LONG_BITS_NAN = strrev(pack('H*', '7ff8000000000000'));
        self::$LONG_BITS_POS_INF = strrev(pack('H*', '7ff0000000000000'));
        self::$LONG_BITS_NEG_INF = strrev(pack('H*', 'fff0000000000000'));
        self::$INT_BITS_NAN = strrev(pack('H*', '7fc00000'));
        self::$INT_BITS_POS_INF = strrev(pack('H*', '7f800000'));
        self::$INT_BITS_NEG_INF = strrev(pack('H*', 'ff800000'));
    }

    public function testSpecialValues()
    {
        $this->assertTrue(is_float(self::$FLOAT_NAN), 'float NaN is a float');
        $this->assertTrue(is_nan(self::$FLOAT_NAN), 'float NaN is NaN');
        $this->assertFalse(is_infinite(self::$FLOAT_NAN), 'float NaN is not infinite');

        $this->assertTrue(is_float(self::$FLOAT_POS_INF), 'float pos infinity is a float');
        $this->assertTrue(is_infinite(self::$FLOAT_POS_INF), 'float pos infinity is infinite');
        $this->assertTrue(0 < self::$FLOAT_POS_INF, 'float pos infinity is greater than 0');
        $this->assertFalse(is_nan(self::$FLOAT_POS_INF), 'float pos infinity is not NaN');

        $this->assertTrue(is_float(self::$FLOAT_NEG_INF), 'float neg infinity is a float');
        $this->assertTrue(is_infinite(self::$FLOAT_NEG_INF), 'float neg infinity is infinite');
        $this->assertTrue(0 > self::$FLOAT_NEG_INF, 'float neg infinity is less than 0');
        $this->assertFalse(is_nan(self::$FLOAT_NEG_INF), 'float neg infinity is not NaN');

        $this->assertTrue(is_double(self::$DOUBLE_NAN), 'double NaN is a double');
        $this->assertTrue(is_nan(self::$DOUBLE_NAN), 'double NaN is NaN');
        $this->assertFalse(is_infinite(self::$DOUBLE_NAN), 'double NaN is not infinite');

        $this->assertTrue(is_double(self::$DOUBLE_POS_INF), 'double pos infinity is a double');
        $this->assertTrue(is_infinite(self::$DOUBLE_POS_INF), 'double pos infinity is infinite');
        $this->assertTrue(0 < self::$DOUBLE_POS_INF, 'double pos infinity is greater than 0');
        $this->assertFalse(is_nan(self::$DOUBLE_POS_INF), 'double pos infinity is not NaN');

        $this->assertTrue(is_double(self::$DOUBLE_NEG_INF), 'double neg infinity is a double');
        $this->assertTrue(is_infinite(self::$DOUBLE_NEG_INF), 'double neg infinity is infinite');
        $this->assertTrue(0 > self::$DOUBLE_NEG_INF, 'double neg infinity is less than 0');
        $this->assertFalse(is_nan(self::$DOUBLE_NEG_INF), 'double neg infinity is not NaN');
    }

    /**
     * @dataProvider specialValsProvider
     */
    public function testEncodingSpecialValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @dataProvider nanValsProvider
     */
    public function testEncodingNanValues($type, $val, $bits)
    {
        $this->assertEncodeNanValues($type, $val, $bits);
    }


    /**
     * @dataProvider floatValsProvider
     */
    public function testEncodingFloatValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @dataProvider doubleValsProvider
     */
    public function testEncodingDoubleValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @param $type
     * @param $val
     * @param $bits
     */
    protected function assertEncodeValues($type, $val, $bits)
    {
        if ($type === self::FLOAT_TYPE) {
            $decoder = ['Avro\\Datum\\BinaryDecoder', 'intBitsToFloat'];
            $encoder = ['Avro\\Datum\\BinaryEncoder', 'floatToIntBits'];
        } else {
            $decoder = ['Avro\\Datum\\BinaryDecoder', 'longBitsToDouble'];
            $encoder = ['Avro\\Datum\\BinaryEncoder', 'doubleToLongBits'];
        }

        $decodedBitsVal = call_user_func($decoder, $bits);
        $this->assertEquals(
            $val,
            $decodedBitsVal,
            sprintf("%s\n expected: '%f'\n    given: '%f'", 'DECODED BITS', $val, $decodedBitsVal)
        );

        $encodedValBits = call_user_func($encoder, $val);
        $this->assertEquals(
            $bits,
            $encodedValBits,
            sprintf(
                "%s\n expected: '%s'\n    given: '%s'",
                'ENCODED VAL',
                Debug::hexString($bits),
                Debug::hexString($encodedValBits)
            )
        );

        $roundTripValue = call_user_func($decoder, $encodedValBits);
        $this->assertEquals(
            $val,
            $roundTripValue,
            sprintf("%s\n expected: '%f'\n     given: '%f'", 'ROUND TRIP BITS', $val, $roundTripValue));
    }

    /**
     * @param $type
     * @param $val
     * @param $bits
     */
    protected function assertEncodeNanValues($type, $val, $bits)
    {
        if (self::FLOAT_TYPE == $type) {
            $decoder = ['Avro\\Datum\\BinaryDecoder', 'intBitsToFloat'];
            $encoder = ['Avro\\Datum\\BinaryEncoder', 'floatToIntBits'];
        } else {
            $decoder = ['Avro\\Datum\\BinaryDecoder', 'longBitsToDouble'];
            $encoder = ['Avro\\Datum\\BinaryEncoder', 'doubleToLongBits'];
        }

        $decodedBitsVal = call_user_func($decoder, $bits);
        $this->assertTrue(
            is_nan($decodedBitsVal),
            sprintf("%s\n expected: '%f'\n    given: '%f'", 'DECODED BITS', $val, $decodedBitsVal)
        );

        $encodedValBits = call_user_func($encoder, $val);
        $this->assertEquals(
            $bits,
            $encodedValBits,
            sprintf(
                "%s\n expected: '%s'\n    given: '%s'",
                'ENCODED VAL',
                Debug::hexString($bits),
                Debug::hexString($encodedValBits)
            )
        );

        $roundTripValue = call_user_func($decoder, $encodedValBits);
        $this->assertTrue(
            is_nan($roundTripValue),
            sprintf("%s\n expected: '%f'\n     given: '%f'", 'ROUND TRIP BITS', $val, $roundTripValue)
        );
    }

    /**
     * @return array
     */
    public function specialValsProvider()
    {
        $this->makeSpecialVals();
        return [
            [self::DOUBLE_TYPE, self::$DOUBLE_POS_INF, self::$LONG_BITS_POS_INF],
            [self::DOUBLE_TYPE, self::$DOUBLE_NEG_INF, self::$LONG_BITS_NEG_INF],
            [self::FLOAT_TYPE, self::$FLOAT_POS_INF, self::$INT_BITS_POS_INF],
            [self::FLOAT_TYPE, self::$FLOAT_NEG_INF, self::$INT_BITS_NEG_INF],
        ];
    }

    /**
     * @return array
     */
    public function normal_vals_provider()
    {
        return [
            [self::DOUBLE_TYPE, (double)-10, "\000\000\000\000\000\000$\300", '000000000000420c'],
            [self::DOUBLE_TYPE, (double)-9, "\000\000\000\000\000\000\"\300", '000000000000220c'],
            [self::DOUBLE_TYPE, (double)-8, "\000\000\000\000\000\000 \300", '000000000000020c'],
            [self::DOUBLE_TYPE, (double)-7, "\000\000\000\000\000\000\034\300", '000000000000c10c'],
            [self::DOUBLE_TYPE, (double)-6, "\000\000\000\000\000\000\030\300", '000000000000810c'],
            [self::DOUBLE_TYPE, (double)-5, "\000\000\000\000\000\000\024\300", '000000000000410c'],
            [self::DOUBLE_TYPE, (double)-4, "\000\000\000\000\000\000\020\300", '000000000000010c'],
            [self::DOUBLE_TYPE, (double)-3, "\000\000\000\000\000\000\010\300", '000000000000800c'],
            [self::DOUBLE_TYPE, (double)-2, "\000\000\000\000\000\000\000\300", '000000000000000c'],
            [self::DOUBLE_TYPE, (double)-1, "\000\000\000\000\000\000\360\277", '0000000000000ffb'],
            [self::DOUBLE_TYPE, (double)0, "\000\000\000\000\000\000\000\000", '0000000000000000'],
            [self::DOUBLE_TYPE, (double)1, "\000\000\000\000\000\000\360?", '0000000000000ff3'],
            [self::DOUBLE_TYPE, (double)2, "\000\000\000\000\000\000\000@", '0000000000000004'],
            [self::DOUBLE_TYPE, (double)3, "\000\000\000\000\000\000\010@", '0000000000008004'],
            [self::DOUBLE_TYPE, (double)4, "\000\000\000\000\000\000\020@", '0000000000000104'],
            [self::DOUBLE_TYPE, (double)5, "\000\000\000\000\000\000\024@", '0000000000004104'],
            [self::DOUBLE_TYPE, (double)6, "\000\000\000\000\000\000\030@", '0000000000008104'],
            [self::DOUBLE_TYPE, (double)7, "\000\000\000\000\000\000\034@", '000000000000c104'],
            [self::DOUBLE_TYPE, (double)8, "\000\000\000\000\000\000 @", '0000000000000204'],
            [self::DOUBLE_TYPE, (double)9, "\000\000\000\000\000\000\"@", '0000000000002204'],
            [self::DOUBLE_TYPE, (double)10, "\000\000\000\000\000\000$@", '0000000000004204'],
            [self::DOUBLE_TYPE, (double)-1234.2132, "\007\316\031Q\332H\223\300", '70ec9115ad84390c'],
            [self::DOUBLE_TYPE, (double)-2.11e+25, "\311\260\276J\031t1\305", '9c0beba49147135c'],

            [self::FLOAT_TYPE, (float)-10, "\000\000 \301", '0000021c'],
            [self::FLOAT_TYPE, (float)-9, "\000\000\020\301", '0000011c'],
            [self::FLOAT_TYPE, (float)-8, "\000\000\000\301", '0000001c'],
            [self::FLOAT_TYPE, (float)-7, "\000\000\340\300", '00000e0c'],
            [self::FLOAT_TYPE, (float)-6, "\000\000\300\300", '00000c0c'],
            [self::FLOAT_TYPE, (float)-5, "\000\000\240\300", '00000a0c'],
            [self::FLOAT_TYPE, (float)-4, "\000\000\200\300", '0000080c'],
            [self::FLOAT_TYPE, (float)-3, "\000\000@\300", '0000040c'],
            [self::FLOAT_TYPE, (float)-2, "\000\000\000\300", '0000000c'],
            [self::FLOAT_TYPE, (float)-1, "\000\000\200\277", '000008fb'],
            [self::FLOAT_TYPE, (float)0, "\000\000\000\000", '00000000'],
            [self::FLOAT_TYPE, (float)1, "\000\000\200?", '000008f3'],
            [self::FLOAT_TYPE, (float)2, "\000\000\000@", '00000004'],
            [self::FLOAT_TYPE, (float)3, "\000\000@@", '00000404'],
            [self::FLOAT_TYPE, (float)4, "\000\000\200@", '00000804'],
            [self::FLOAT_TYPE, (float)5, "\000\000\240@", '00000a04'],
            [self::FLOAT_TYPE, (float)6, "\000\000\300@", '00000c04'],
            [self::FLOAT_TYPE, (float)7, "\000\000\340@", '00000e04'],
            [self::FLOAT_TYPE, (float)8, "\000\000\000A", '00000014'],
            [self::FLOAT_TYPE, (float)9, "\000\000\020A", '00000114'],
            [self::FLOAT_TYPE, (float)10, "\000\000 A", '00000214'],
            [self::FLOAT_TYPE, (float)-1234.5, "\000P\232\304", '0005a94c'],
            [self::FLOAT_TYPE, (float)-211300000.0, "\352\202I\315", 'ae2894dc'],
        ];
    }

    /**
     * @return array
     */
    public function nanValsProvider()
    {
        $this->makeSpecialVals();

        return [
            [self::DOUBLE_TYPE, self::$DOUBLE_NAN, self::$LONG_BITS_NAN],
            [self::FLOAT_TYPE, self::$FLOAT_NAN, self::$INT_BITS_NAN],
        ];
    }

    /**
     * @return array
     */
    public function floatValsProvider()
    {
        $array = [];

        foreach ($this->normal_vals_provider() as $values) {
            if (self::FLOAT_TYPE === $values[0]) {
                $array[] = [$values[0], $values[1], $values[2]];
            }
        }

        return $array;
    }

    /**
     * @return array
     */
    public function doubleValsProvider()
    {
        $array = [];

        foreach ($this->normal_vals_provider() as $values) {
            if (self::DOUBLE_TYPE === $values[0]) {
                $array[] = [$values[0], $values[1], $values[2]];
            }
        }

        return $array;
    }
}