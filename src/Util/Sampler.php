<?php

namespace RedisBackup\Util;

class Sampler
{
    static $times = array();
    static $counter = array();

    public static function sample($rate, $name = '')
    {
        if (!isset(self::$times[$name])) {
            self::$times[$name] = 0;
            self::$counter[$name] = 0;
        }
        ++self::$times[$name];
        ++self::$counter[$name];
        if (self::$times[$name] >= $rate) {
            self::$times[$name] = 0;
            return true;
        }
        return false;
    }

    public static function stop($name = '')
    {
        unset(static::$times[$name]);
        unset(static::$counter[$name]);
    }
}
