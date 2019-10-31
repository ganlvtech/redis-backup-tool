<?php

namespace RedisBackup\Util;

class Logger
{
    public static $path;
    public static $error_path;

    public static function init($dir)
    {
        static::$path = $dir . '/' . date('Ymd_H') . '.log';
        static::$error_path = $dir . '/' . date('Ymd_H') . '_error.log';
    }

    public static function info($str)
    {
        file_put_contents(static::$path, date(DATE_ATOM) . ' [INFO] ' . $str . "\n", FILE_APPEND);
        echo '[INFO] ', $str, PHP_EOL;
    }

    public static function error($str)
    {
        file_put_contents(static::$error_path, date(DATE_ATOM) . ' [ERROR] ' . $str . "\n", FILE_APPEND);
        file_put_contents(static::$path, date(DATE_ATOM) . ' [ERROR] ' . $str . "\n", FILE_APPEND);
        echo '[ERROR] ', $str, PHP_EOL;
    }
}
