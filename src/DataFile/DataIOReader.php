<?php

namespace Avro\DataFile;

use Avro\Datum\BinaryDecoder;
use Avro\Datum\Reader;
use Avro\Exception\DataIOException;
use Avro\Exception\AvroException;
use Avro\Exception\IOSchemaMatchException;
use Avro\Exception\SchemaParseException;
use Avro\IO\IOInterface;
use Avro\Schema\AbstractSchema;
use Avro\Util\Helpers;

class DataIOReader
{
    /**
     * @var IOInterface
     */
    private $io;

    /**
     * @var BinaryDecoder
     */
    private $decoder;

    /**
     * @var Reader
     */
    private $datumReader;

    /**
     * @var string
     */
    private $syncMarker;

    /**
     * Object container metadata.
     * @var array
     */
    private $metadata;

    /**
     * Count of items in block.
     * @var int
     */
    private $blockCount;

    /**
     * @param IOInterface $io
     * @param Reader $datumReader
     * @throws DataIOException
     * @throws AvroException
     * @throws SchemaParseException
     */
    public function __construct(IOInterface $io, Reader $datumReader)
    {
        if (!($io instanceof IOInterface)) {
            throw new DataIOException('io must be instance of AvroIO');
        }

        $this->io = $io;
        $this->decoder = new BinaryDecoder($this->io);
        $this->datumReader = $datumReader;
        $this->readHeader();

        $codec = Helpers::arrayValue($this->metadata, DataIO::METADATA_CODEC_ATTR);
        if ($codec && !DataIO::isValidCodec($codec)) {
            throw new DataIOException(sprintf('Unknown codec: %s', $codec));
        }

        $this->blockCount = 0;
        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datumReader->setWritersSchema(
            AbstractSchema::parse($this->metadata[DataIO::METADATA_SCHEMA_ATTR])
        );
    }

    public function getSyncMarker()
    {
        return $this->syncMarker;
    }

    public function getMetadata()
    {
        return $this->metadata;
    }

    /**
     * Reads header of object container.
     * @throws AvroException
     * @throws DataIOException
     * @throws SchemaParseException
     * @throws IOSchemaMatchException
     */
    private function readHeader()
    {
        $this->seek(0, IOInterface::SEEK_SET);

        $magic = $this->read(DataIO::magicSize());

        if (strlen($magic) < DataIO::magicSize()) {
            throw new DataIOException('Not an Avro data file: shorter than the Avro magic block');
        }

        if (DataIO::magic() !== $magic)
            throw new DataIOException(
                sprintf(
                    'Not an Avro data file: %s does not match %s',
                    $magic,
                    DataIO::magic()
                )
            );

        $this->metadata = $this->datumReader->readData(
            DataIO::metadataSchema(),
            DataIO::metadataSchema(),
            $this->decoder
        );
        $this->syncMarker = $this->read(DataIO::SYNC_SIZE);
    }

    /**
     * @return array of data from object container.
     * @throws AvroException
     * @throws IOSchemaMatchException
     * @internal Would be nice to implement data() as an iterator, I think
     */
    public function data()
    {
        $data = [];
        while (true) {
            if ($this->blockCount === 0) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync()) {
                    if ($this->isEof()) {
                        break;
                    }
                }

                $this->readBlockHeader();
            }
            $data[] = $this->datumReader->read($this->decoder);
            $this->blockCount -= 1;
        }

        return $data;
    }

    /**
     * Closes this writer (and its IOInterface object.)
     */
    public function close()
    {
        return $this->io->close();
    }

    /**
     * @param $offset
     * @param $whence
     * @return bool
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @param $len
     * @return string
     */
    private function read($len)
    {
        return $this->io->read($len);
    }

    private function isEof()
    {
        return $this->io->isEof();
    }

    /**
     * @return bool
     */
    private function skipSync()
    {
        $proposedSyncMarker = $this->read(DataIO::SYNC_SIZE);
        if ($proposedSyncMarker !== $this->syncMarker) {
            $this->seek(-DataIO::SYNC_SIZE, IOInterface::SEEK_CUR);
            return false;
        }
        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block
     * and the length in bytes of the block)
     * @return int length in bytes of the block.
     */
    private function readBlockHeader()
    {
        $this->blockCount = $this->decoder->readLong();
        return $this->decoder->readLong();
    }

}