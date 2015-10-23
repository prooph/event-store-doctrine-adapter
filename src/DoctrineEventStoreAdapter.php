<?php
/*
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) 2014-2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Prooph\EventStore\Adapter\Doctrine;

use DateTimeInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Schema\Schema;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageDataAssertion;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Adapter\PayloadSerializer;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

/**
 * EventStore Adapter for Doctrine
 */
final class DoctrineEventStoreAdapter implements Adapter, CanHandleTransaction
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * Custom sourceType to table mapping
     *
     * @var array
     */
    private $streamTableMap = [];

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    /**
     * Serialize adapter used to serialize event payload
     *
     * @var PayloadSerializer
     */
    private $payloadSerializer;

    /**
     * @param Connection $dbalConnection
     * @param MessageFactory $messageFactory
     * @param MessageConverter $messageConverter
     * @param PayloadSerializer $payloadSerializer
     * @param array $streamTableMap
     */
    public function __construct(
        Connection $dbalConnection,
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PayloadSerializer $payloadSerializer,
        array $streamTableMap = [])
    {
        $this->connection = $dbalConnection;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
        $this->streamTableMap = $streamTableMap;
    }

    /**
     * @param Stream $stream
     * @throws \Prooph\EventStore\Exception\RuntimeException If creation of stream fails
     * @return void
     */
    public function create(Stream $stream)
    {
        if (!$stream->streamEvents()->valid()) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $firstEvent = $stream->streamEvents()->current();

        $this->createSchemaFor($stream->streamName(), $firstEvent->metadata());

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param Iterator $streamEvents
     * @throws \Prooph\EventStore\Exception\StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents)
    {
        foreach ($streamEvents as $event) {
            $this->insertEvent($streamName, $event);
        }
    }

    /**
     * @param StreamName $streamName
     * @param null|int $minVersion
     * @return Stream|null
     */
    public function load(StreamName $streamName, $minVersion = null)
    {
        $events = $this->loadEvents($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return Iterator
     */
    public function loadEvents(StreamName $streamName, array $metadata = [], $minVersion = null)
    {
        $queryBuilder = $this->connection->createQueryBuilder();

        $table = $this->getTable($streamName);

        $queryBuilder
            ->select('*')
            ->from($table, $table)
            ->orderBy('version', 'ASC');

        foreach ($metadata as $key => $value) {
            $queryBuilder->andWhere($key . ' = :value'.$key)
                ->setParameter('value'.$key, (string)$value);
        }

        if (null !== $minVersion) {
            $queryBuilder
                ->andWhere('version >= :version')
                ->setParameter('version', $minVersion);
        }

        return new DoctrineStreamIterator($queryBuilder, $this->messageFactory, $this->payloadSerializer, $metadata);
    }

    /**
     * @param StreamName $streamName
     * @param DateTimeInterface|null $since
     * @param array $metadata
     * @return DoctrineStreamIterator
     */
    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = [])
    {
        $queryBuilder = $this->connection->createQueryBuilder();

        $table = $this->getTable($streamName);

        $queryBuilder
            ->select('*')
            ->from($table, $table)
            ->orderBy('created_at', 'ASC')
            ->addOrderBy('version', 'ASC');

        foreach ($metadata as $key => $value) {
            $queryBuilder->andWhere($key . ' = :value'.$key)
                ->setParameter('value'.$key, (string)$value);
        }

        if (null !== $since) {
            $queryBuilder->andWhere('created_at > :createdAt')
                ->setParameter('createdAt', $since->format('Y-m-d\TH:i:s.u'));
        }

        return new DoctrineStreamIterator($queryBuilder, $this->messageFactory, $this->payloadSerializer, $metadata);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param bool $returnSql
     * @return array|void If $returnSql is set to true then method returns array of SQL strings
     */
    public function createSchemaFor(StreamName $streamName, array $metadata, $returnSql = false)
    {
        $schema = new Schema();

        static::addToSchema($schema, $this->getTable($streamName), $metadata);

        $sqls = $schema->toSql($this->connection->getDatabasePlatform());

        if ($returnSql) {
            return $sqls;
        }

        foreach ($sqls as $sql) {
            $this->connection->executeQuery($sql);
        }
    }

    /**
     * @param Schema $schema
     * @param string $table
     * @param array $metadata
     */
    public static function addToSchema(Schema $schema, $table, array $metadata)
    {
        $table = $schema->createTable($table);

        $table->addColumn('event_id', 'string', ['length' => 36]);

        $table->addColumn('version', 'integer');
        $table->addColumn('event_name', 'string', ['length' => 100]);
        $table->addColumn('payload', 'text');
        $table->addColumn('created_at', 'string', ['length' => 50]);

        foreach ($metadata as $key => $value) {
            $table->addColumn($key, 'string', ['length' => 100]);
        }

        $table->setPrimaryKey(['event_id']);
    }

    /**
     * Begin transaction
     */
    public function beginTransaction()
    {
        if (0 != $this->connection->getTransactionNestingLevel()) {
            throw new \RuntimeException('Transaction already started');
        }

        $this->connection->beginTransaction();
    }

    /**
     * Commit transaction
     */
    public function commit()
    {
        $this->connection->commit();
    }

    /**
     * Rollback transaction
     */
    public function rollback()
    {
        $this->connection->rollBack();
    }

    /**
     * Get table name for given stream name
     *
     * @param StreamName $streamName
     * @return string
     */
    public function getTable(StreamName $streamName)
    {
        if (isset($this->streamTableMap[$streamName->toString()])) {
            $tableName = $this->streamTableMap[$streamName->toString()];
        } else {
            $tableName = strtolower($this->getShortStreamName($streamName));

            if (strpos($tableName, "_stream") === false) {
                $tableName.= "_stream";
            }
        }

        return $tableName;
    }

    /**
     * @return Connection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * Insert an event
     *
     * @param StreamName $streamName
     * @param Message $e
     * @return void
     */
    private function insertEvent(StreamName $streamName, Message $e)
    {
        $eventArr = $this->messageConverter->convertToArray($e);

        MessageDataAssertion::assert($eventArr);

        $eventData = [
            'event_id' => $eventArr['uuid'],
            'version' => $eventArr['version'],
            'event_name' => $eventArr['message_name'],
            'payload' => $this->payloadSerializer->serializePayload($eventArr['payload']),
            'created_at' => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        foreach ($eventArr['metadata'] as $key => $value) {
            $eventData[$key] = (string)$value;
        }

        $this->connection->insert($this->getTable($streamName), $eventData);
    }

    /**
     * @param StreamName $streamName
     * @return string
     */
    private function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }
}
