<?php

namespace Avro\IO;

interface IOInterface
{
    /**
     * General read mode.
     * @var string
     */
    const READ_MODE = 'r';

    /**
     * General write mode.
     * @var string
     */
    const WRITE_MODE = 'w';

    /**
     * Set position equal to $offset bytes.
     * @var int
     */
    const SEEK_CUR = SEEK_CUR;

    /**
     * Set position to current index + $offset bytes.
     * @var int
     */
    const SEEK_SET = SEEK_SET;

    /**
     * Set position to end of file + $offset bytes.
     * @var int
     */
    const SEEK_END = SEEK_END;

    public function read($len);

    /**
     * @param $data
     * @return int
     */
    public function write($data);

    public function tell();

    public function seek($offset, $whence = self::SEEK_SET);

    public function flush();

    public function isEof();

    public function close();
}