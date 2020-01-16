<?php

namespace Avro\Util;

class Helpers
{
    /**
     * Determines whether the given array is an associative array
     * (what is termed a map, hash, or dictionary in other languages)
     * or a list (an array with monotonically increasing integer indicies
     * starting with zero).
     *
     * @param $array array to test
     * @return bool
     */
    public static function isList($array)
    {
        if (is_array($array))  {
            $i = 0;
            foreach ($array as $key => $value) {
                if ($i !== $key) {
                    return false;
                }
                $i++;
            }
            return true;
        }
        return false;
    }

    /**
     * @param array $array
     * @param string $key
     * @return mixed
     */
    public static function arrayValue(array $array, $key)
    {
        return isset($array[$key]) ? $array[$key] : null;
    }
}