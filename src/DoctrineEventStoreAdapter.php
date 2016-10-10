<?php
/**
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) 2014-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2015-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\Doctrine;

use DateTimeInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Schema\Schema;
use Iterator;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageDataAssertion;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Adapter\PayloadSerializer;
use Prooph\EventStore\Adapter\Exception\RuntimeException;
use Prooph\EventStore\Exception\ConcurrencyException;
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
     * @var int
     */
    private $loadBatchSize;

    public function __construct(
        Connection $dbalConnection,
        MessageFactory $messageFactory,
        MessageConverter $messageConverter,
        PayloadSerializer $payloadSerializer,
        array $streamTableMap = [],
        int $loadBatchSize = 10000
    ) {
        $this->connection = $dbalConnection;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
        $this->payloadSerializer = $payloadSerializer;
        $this->streamTableMap = $streamTableMap;
        $this->loadBatchSize = $loadBatchSize;
    }

    /**
     * @throws RuntimeException If creation of stream fails
     */
    public function create(Stream $stream): void
    {
        if (! $stream->streamEvents()->valid()) {
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
     * @throws ConcurrencyException
     */
    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        try {
            foreach ($streamEvents as $event) {
                $this->insertEvent($streamName, $event);
            }
        } catch (UniqueConstraintViolationException $e) {
            throw new ConcurrencyException('At least one event with same version exists already', 0, $e);
        }
    }

    public function load(StreamName $streamName, int $minVersion = null): ?Stream
    {
        $events = $this->loadEvents($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    public function loadEvents(StreamName $streamName, array $metadata = [], int $minVersion = null): Iterator
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

        return new DoctrineStreamIterator(
            $queryBuilder,
            $this->messageFactory,
            $this->payloadSerializer,
            $metadata,
            $this->loadBatchSize
        );
    }

    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = []): Iterator
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

        return new DoctrineStreamIterator(
            $queryBuilder,
            $this->messageFactory,
            $this->payloadSerializer,
            $metadata,
            $this->loadBatchSize
        );
    }

    public function createSchemaFor(StreamName $streamName, array $metadata): void
    {
        $sqls = $this->getSqlSchemaFor($streamName, $metadata);

        foreach ($sqls as $sql) {
            $this->connection->executeQuery($sql);
        }
    }

    public function getSqlSchemaFor(StreamName $streamName, array $metadata): array
    {
        $schema = new Schema();

        static::addToSchema($schema, $this->getTable($streamName), $metadata);

        return $schema->toSql($this->connection->getDatabasePlatform());
    }

    public static function addToSchema(Schema $schema, string $table, array $metadata): void
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

        if ($table->hasColumn('aggregate_id')) {
            $table->addUniqueIndex(['aggregate_id', 'version']);
        }

        $table->setPrimaryKey(['event_id']);
    }

    public function beginTransaction(): void
    {
        if (0 != $this->connection->getTransactionNestingLevel()) {
            throw new RuntimeException('Transaction already started');
        }

        $this->connection->beginTransaction();
    }

    public function commit(): void
    {
        $this->connection->commit();
    }

    public function rollback(): void
    {
        $this->connection->rollBack();
    }

    public function getTable(StreamName $streamName): string
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

    public function getConnection(): Connection
    {
        return $this->connection;
    }

    private function insertEvent(StreamName $streamName, Message $e): void
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

    private function getShortStreamName(StreamName $streamName): string
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }
}
