<?php

namespace RedisBackup\Util;

class FileBackup
{
    /**
     * 把一个已存在的文件备份，空出这个文件名
     *
     * @param string $path
     * @param bool $keep 是否保留原文件
     *
     * @return bool
     */
    public static function backup($path, $keep = false)
    {
        if (!file_exists($path)) {
            return true;
        }
        $dot_pos = strrpos($path, '.');
        if ($dot_pos === false) {
            $filename = $path;
            $ext = '';
        } else {
            $filename = substr($path, 0, $dot_pos);
            $ext = substr($path, $dot_pos);
        }
        $new_filename_base = $filename . '_' . date('Ymd_His', fileatime($path));
        $new_filename = $new_filename_base;
        $found_path = null;
        for ($i = 1; $i <= 100; ++$i) {
            if (!file_exists($new_filename . $ext)) {
                $found_path = $new_filename . $ext;
                break;
            }
            $new_filename = $new_filename_base . '_' . sprintf('%02d', $i);
        }
        if ($found_path) {
            if ($keep) {
                return copy($path, $found_path);
            } else {
                return rename($path, $found_path);
            }
        }
        return false;
    }
}
