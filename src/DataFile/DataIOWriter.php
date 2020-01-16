<?php

namespace Avro\DataFile;

use Avro\Datum\BinaryEncoder;
use Avro\Datum\Reader;
use Avro\Datum\Writer;
use Avro\Exception\DataIOException;
use Avro\Exception\AvroException;
use Avro\Exception\IOException;
use Avro\Exception\IOTypeException;
use Avro\Exception\SchemaParseException;
use Avro\IO\IOInterface;
use Avro\IO\StringIO;
use Avro\Schema\AbstractSchema;
use Avro\Schema\SchemaInterface;

class DataIOWriter
{
    /**
     * @var IOInterface
     */
    private $io;

    /**
     * @var BinaryEncoder
     */
    private $encoder;

    /**
     * @var Writer
     */
    private $datumWriter;

    /**
     * @var StringIO
     */
    private $buffer;

    /**
     * @var BinaryEncoder
     */
    private $bufferEncoder;

    /**
     * Count of items written to block.
     * @var int
     */
    private $blockCount;

    /**
     * Map of object container metadata.
     * @var array
     */
    private $metadata;

    /**
     * @var string
     */
    private $syncMarker;

    /**
     * @param IOInterface $io
     * @param Writer $datumWriter
     * @param $writersSchema
     * @throws DataIOException
     * @throws AvroException
     * @throws IOException
     * @throws SchemaParseException
     */
    public function __construct(IOInterface $io, Writer $datumWriter, $writersSchema = null)
    {
        $this->io = $io;
        $this->encoder = new BinaryEncoder($this->io);
        $this->datumWriter = $datumWriter;
        $this->buffer = new StringIO();
        $this->bufferEncoder = new BinaryEncoder($this->buffer);
        $this->blockCount = 0;
        $this->metadata = [];

        if ($writersSchema) {
            $this->syncMarker = self::generateSyncMarker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = DataIO::NULL_CODEC;
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = strval($writersSchema);
            $this->writeHeader();
        } else {
            $dfr = new DataIOReader($this->io, new Reader());
            $this->syncMarker = $dfr->getSyncMarker();
            $this->metadata[DataIO::METADATA_CODEC_ATTR] = $dfr->getMetadata()[DataIO::METADATA_CODEC_ATTR];

            $schemaFromFile = $dfr->getMetadata()[DataIO::METADATA_SCHEMA_ATTR];
            $this->metadata[DataIO::METADATA_SCHEMA_ATTR] = $schemaFromFile;
            $this->datumWriter->setWritersSchema(AbstractSchema::parse($schemaFromFile));
            $this->seek(0, SEEK_END);
        }
    }

    /**
     * @param mixed $datum
     * @throws AvroException
     */
    public function append($datum)
    {
        $this->datumWriter->write($datum, $this->bufferEncoder);
        $this->blockCount++;

        if ($this->buffer->getLength() >= DataIO::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * Flushes buffer to AvroIO object container and closes it.
     * @return mixed
     * @throws DataIOException
     * @throws IOException
     */
    public function close()
    {
        $this->flush();
        return $this->io->close();
    }

    /**
     * Flushes buffer to AvroIO object container.
     * @return mixed
     * @throws DataIOException
     * @throws IOException
     */
    private function flush()
    {
        $this->writeBlock();
        return $this->io->flush();
    }

    /**
     * Writes a block of data to the AvroIO object container.
     * @throws DataIOException
     * @throws IOException
     */
    private function writeBlock()
    {
        if ($this->blockCount > 0) {
            $this->encoder->writeLong($this->blockCount);
            $toWrite = strval($this->buffer);
            $this->encoder->writeLong(strlen($toWrite));

            if (DataIO::isValidCodec($this->metadata[DataIO::METADATA_CODEC_ATTR])) {
                $this->write($toWrite);
            } else {
                throw new DataIOException(
                    sprintf(
                        'codec %s is not supported',
                        $this->metadata[DataIO::METADATA_CODEC_ATTR]
                    )
                );
            }

            $this->write($this->syncMarker);
            $this->buffer->truncate();
            $this->blockCount = 0;
        }
    }

    /**
     * Writes the header of the AvroIO object container
     * @throws AvroException
     * @throws IOTypeException
     */
    private function writeHeader()
    {
        $this->write(DataIO::magic());
        $this->datumWriter->writeData(DataIO::metadataSchema(), $this->metadata, $this->encoder);
        $this->write($this->syncMarker);
    }

    /**
     * @param string $bytes
     * @return int
     */
    private function write($bytes)
    {
        return $this->io->write($bytes);
    }

    /**
     * @param int $offset
     * @param int $whence
     * @return bool
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * Generate  a new, unique sync marker.
     * @return string
     */
    private static function generateSyncMarker()
    {
        // From http://php.net/manual/en/function.mt-rand.php comments
        return pack('S8',
            mt_rand(0, 0xffff), mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) | 0x4000,
            mt_rand(0, 0xffff) | 0x8000,
            mt_rand(0, 0xffff), mt_rand(0, 0xffff), mt_rand(0, 0xffff)
        );
    }
}