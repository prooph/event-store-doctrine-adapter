<?php

namespace Prooph\EventStore\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use Prooph\EventStore\Adapter\AdapterInterface;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Adapter\Exception\InvalidArgumentException;
use Prooph\EventStore\Adapter\Feature\TransactionFeatureInterface;
use Prooph\EventStore\Stream\AggregateType;
use Prooph\EventStore\Stream\EventId;
use Prooph\EventStore\Stream\EventName;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamEvent;
use Prooph\EventStore\Stream\StreamId;
use Zend\Serializer\Serializer;

/**
 * EventStore Adapter for Doctrine
 */
class DoctrineEventStoreAdapter implements AdapterInterface, TransactionFeatureInterface
{
    /**
     * @var Connection
     */
    protected $connection;

    /**
     * Custom sourceType to table mapping
     *
     * @var array
     */
    protected $aggregateTypeTableMap = array();

    /**
     * Name of the table that contains snapshot metadata
     *
     * @var string
     */
    protected $snapshotTable = 'snapshot';

    /**
     * Serialize adapter used to serialize event payload
     *
     * @var string|\Zend\Serializer\Adapter\AdapterInterface
     */
    protected $serializerAdapter;

    /**
     * @param  array                                                       $configuration
     * @throws \Prooph\EventStore\Adapter\Exception\ConfigurationException
     */
    public function __construct(array $configuration)
    {
        if (!isset($configuration['connection'])) {
            throw new ConfigurationException('DB connection configuration is missing');
        }

        if (isset($configuration['source_table_map'])) {
            $this->aggregateTypeTableMap = $configuration['source_table_map'];
        }

        if (isset($configuration['snapshot_table'])) {
            $this->snapshotTable = $configuration['snapshot_table'];
        }

        $connection = $configuration['connection'];

        if (!$connection instanceof Connection) {
            $connection = DriverManager::getConnection($connection);
        }

        $this->connection = $connection;

        if (isset($configuration['serializer_adapter'])) {
            $this->serializerAdapter = $configuration['serializer_adapter'];
        }
    }

    /**
     * @param  AggregateType                                                 $aggregateType
     * @param  StreamId                                                      $streamId
     * @param  null|int                                                      $version
     * @return Stream
     * @throws \Prooph\EventStore\Adapter\Exception\InvalidArgumentException
     */
    public function loadStream(AggregateType $aggregateType, StreamId $streamId, $version = null)
    {
        try {
            \Assert\that($version)->nullOr()->integer();
        } catch (\InvalidArgumentException $ex) {
            throw new InvalidArgumentException(
                sprintf(
                    'Loading the stream for AggregateType %s with id %s failed cause invalid parameters were passed: %s',
                    $aggregateType->toString(),
                    $streamId->toString(),
                    $ex->getMessage()
                )
            );
        }

        $queryBuilder = $this->connection->createQueryBuilder();

        $table = $this->getTable($aggregateType);

        $queryBuilder
            ->select('*')
            ->from($table, $table)
            ->where('streamId = :streamId')
            ->setParameter('streamId', $streamId->toString())
            ->orderBy('version', 'ASC');

        if (!is_null($version)) {
            $queryBuilder
                ->where('version >= :version')
                ->setParameter('version', $version);
        }

        /* @var $stmt \Doctrine\DBAL\Statement */
        $stmt = $queryBuilder->execute();

        $events = array();

        foreach ($stmt->fetchAll() as $eventData) {
            $payload = Serializer::unserialize($eventData['payload'], $this->serializerAdapter);

            $eventId = new EventId($eventData['eventId']);

            $eventName = new EventName($eventData['eventName']);

            $occurredOn = new \DateTime($eventData['occurredOn']);

            $events[] = new StreamEvent($eventId, $eventName, $payload, (int) $eventData['version'], $occurredOn);
        }

        return new Stream($aggregateType, $streamId, $events);
    }

    /**
     * Add new stream to the source stream
     *
     * @param Stream $stream
     *
     * @return void
     */
    public function addToExistingStream(Stream $stream)
    {
        foreach ($stream->streamEvents() as $event) {
            $this->insertEvent($stream->aggregateType(), $stream->streamId(), $event);
        }
    }

    /**
     * @param AggregateType $aggregateType
     * @param StreamId      $streamId
     */
    public function removeStream(AggregateType $aggregateType, StreamId $streamId)
    {
        $table = $this->getTable($aggregateType);

        $this->connection->createQueryBuilder()
            ->delete($table)
            ->where('streamId = :streamId')
            ->setParameter('streamId', $streamId->toString())
            ->execute();
    }

    /**
     * @param  array                   $streams
     * @return bool
     * @throws \BadMethodCallException
     */
    public function createSchema(array $streams)
    {
        $schema = $this->connection->getSchemaManager()->createSchema();

        foreach ($streams as $stream) {
            static::addToSchema($schema, $this->getTable(new AggregateType($stream)));
        }

        $sqls = $schema->toSql($this->connection->getDatabasePlatform());

        foreach ($sqls as $sql) {
            $this->connection->executeQuery($sql);
        }
    }

    /**
     * @param array $streams
     */
    public function dropSchema(array $streams)
    {
        $schema = $this->connection->getSchemaManager()->createSchema();

        foreach ($streams as $stream) {
            static::addToSchema($schema, $this->getTable(new AggregateType($stream)));
        }

        $sqls = $schema->toDropSql($this->connection->getDatabasePlatform());

        foreach ($sqls as $sql) {
            $this->connection->executeQuery($sql);
        }
    }

    public static function addToSchema(Schema $schema, $table)
    {
        $table = $schema->createTable($table);

        $table->addColumn('eventId', 'string', array(
            'length' => 200
        ));

        $table->addColumn('streamId', 'string', array(
            'length' => 200
        ));

        $table->addColumn('version', 'integer');
        $table->addColumn('eventName', 'text');
        $table->addColumn('payload', 'text');
        $table->addColumn('occurredOn', 'text');

        $table->setPrimaryKey(array('eventId'));
    }

    public function beginTransaction()
    {
        $this->connection->beginTransaction();
    }

    public function commit()
    {
        $this->connection->commit();
    }

    public function rollback()
    {
        $this->connection->rollBack();
    }

    /**
     * Insert an event
     *
     * @param  \Prooph\EventStore\Stream\AggregateType $aggregateType
     * @param  \Prooph\EventStore\Stream\StreamId      $streamId
     * @param  \Prooph\EventStore\Stream\StreamEvent   $e
     * @return void
     */
    protected function insertEvent(AggregateType $aggregateType, StreamId $streamId, StreamEvent $e)
    {
        $eventData = array(
            'eventId' => $e->eventId()->toString(),
            'streamId' => $streamId->toString(),
            'version' => $e->version(),
            'eventName' => $e->eventName()->toString(),
            'payload' => Serializer::serialize($e->payload(), $this->serializerAdapter),
            'occurredOn' => $e->occurredOn()->format('Y-m-d\TH:i:s.uO')
        );

        $this->connection->insert($this->getTable($aggregateType), $eventData);
    }

    /**
     * Get tablename for given $aggregateFQCN
     *
     * @param  AggregateType $aggregateType
     * @return string
     */
    protected function getTable(AggregateType $aggregateType)
    {
        if (isset($this->aggregateTypeTableMap[$aggregateType->toString()])) {
            $tableName = $this->aggregateTypeTableMap[$aggregateType->toString()];
        } else {
            $tableName = strtolower($this->getShortAggregateType($aggregateType)) . "_stream";
        }

        return $tableName;
    }

    /**
     * @param  AggregateType $aggregateType
     * @return string
     */
    protected function getShortAggregateType(AggregateType $aggregateType)
    {
        return join('', array_slice(explode('\\', $aggregateType->toString()), -1));
    }
}
