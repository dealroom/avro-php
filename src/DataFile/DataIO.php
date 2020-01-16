<?php

namespace Avro\DataFile;

use Avro\Datum\Reader;
use Avro\Datum\Writer;
use Avro\Exception\AvroException;
use Avro\Exception\DataIOException;
use Avro\Exception\IOException;
use Avro\Exception\SchemaParseException;
use Avro\IO\FileIO;
use Avro\IO\IOInterface;
use Avro\Schema\AbstractSchema;
use Avro\Schema\SchemaInterface;

class DataIO
{
    /**
     * Used in file header.
     * @var int
     */
    const VERSION = 1;

    /**
     * Count of bytes in synchronization marker.
     * @var int
     */
    const SYNC_SIZE = 16;

    /**
     * Count of items per block, arbitrarily set to 4000 * SYNC_SIZE.
     * @var int
     * @todo make this value configurable
     */
    const SYNC_INTERVAL = 64000;

    /**
     * Map key for datafile metadata codec value.
     * @var string
     */
    const METADATA_CODEC_ATTR = 'avro.codec';

    /**
     * Map key for datafile metadata schema value.
     * @var string
     */
    const METADATA_SCHEMA_ATTR = 'avro.schema';

    /**
     * JSON for datafile metadata schema.
     * @var string
     */
    const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /**
     * Codec value for NULL codec.
     * @var string
     */
    const NULL_CODEC = 'null';

    /**
     * Codec value for deflate codec.
     * @var string
     */
    const DEFLATE_CODEC = 'deflate';

    /**
     * Array of valid codec names.
     * @var array
     * @todo Avro implementations are required to implement deflate codec as well,
     *       so implement it already!
     */
    private static $valid_codecs = [self::NULL_CODEC];

    /**
     * Cached version of metadata schema object.
     * @var SchemaInterface
     */
    private static $metadataSchema;

    /**
     * The initial "magic" segment of an Avro container file header.
     * @return string
     */
    public static function magic()
    {
        return ('Obj' . pack('c', self::VERSION));
    }

    /**
     * Count of bytes in the initial "magic" segment of the Avro container file header.
     * @return int
     */
    public static function magicSize()
    {
        return strlen(self::magic());
    }

    /**
     * Object of Avro container file metadata.
     * @return SchemaInterface
     * @throws SchemaParseException
     */
    public static function metadataSchema()
    {
        if (is_null(self::$metadataSchema)) {
            self::$metadataSchema = AbstractSchema::parse(self::METADATA_SCHEMA_JSON);
        }

        return self::$metadataSchema;
    }

    /**
     * @param string $filePath file_path of file to open
     * @param string $mode one of AvroFile::READ_MODE or AvroFile::WRITE_MODE
     * @param string $schemaJson JSON of writer's schema
     * @return DataIOReader|DataIOWriter
     * @throws DataIOException
     * @throws IOException
     * @throws SchemaParseException
     * @throws AvroException
     */
    public static function openFile(
        $filePath,
        $mode = FileIO::READ_MODE,
        $schemaJson = null
    )
    {
        $schema = !is_null($schemaJson) ? AbstractSchema::parse($schemaJson) : null;

        switch ($mode) {
            case FileIO::WRITE_MODE:
                if (is_null($schema)) {
                    throw new DataIOException('Writing an Avro file requires a schema.');
                }
                $file = new FileIO($filePath, FileIO::WRITE_MODE);
                $io = self::openWriter($file, $schema);
                break;
            case FileIO::READ_MODE:
                $file = new FileIO($filePath, FileIO::READ_MODE);
                $io = self::openReader($file, $schema);
                break;
            default:
                throw new DataIOException(
                    sprintf(
                        "Only modes '%s' and '%s' allowed. You gave '%s'.",
                        FileIO::READ_MODE,
                        FileIO::WRITE_MODE, $mode
                    )
                );
        }

        return $io;
    }

    /**
     * Get array of valid codecs.
     * @return array
     */
    private static function validCodecs()
    {
        return self::$valid_codecs;
    }

    /**
     * @param string $codec
     * @return bool
     */
    public static function isValidCodec($codec)
    {
        return in_array($codec, self::validCodecs());
    }

    /**
     * @param IOInterface $io
     * @param SchemaInterface $schema
     * @return DataIOWriter
     * @throws DataIOException
     * @throws IOException
     * @throws SchemaParseException
     * @throws AvroException
     */
    protected static function openWriter($io, $schema)
    {
        $writer = new Writer($schema);
        return new DataIOWriter($io, $writer, $schema);
    }

    /**
     * @param IOInterface $io
     * @param SchemaInterface $schema
     * @return DataIOReader
     * @throws DataIOException
     * @throws SchemaParseException
     * @throws AvroException
     */
    protected static function openReader($io, $schema)
    {
        $reader = new Reader(null, $schema);
        return new DataIOReader($io, $reader);
    }

}