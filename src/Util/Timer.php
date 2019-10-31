<?php

namespace RedisBackup\Util;

class Timer
{
    static $t0 = array();

    public static function getNow()
    {
        return microtime(true);
    }

    public static function getStartTime($name = '')
    {
        return static::$t0[$name];
    }

    public static function start($name = '')
    {
        static::$t0[$name] = static::getNow();
    }

    public static function time($precision = 0.01, $name = '')
    {
        $t = static::getNow() - static::getStartTime($name);
        $t = (int)($t / $precision) * $precision;
        return $t;
    }

    public static function stop($name = '')
    {
        unset(static::$t0[$name]);
    }
}
