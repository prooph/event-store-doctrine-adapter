<?php

namespace Prooph\EventStore\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use Prooph\Common\Messaging\DomainEvent;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Adapter\Feature\CanHandleTransaction;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Zend\Serializer\Serializer;

/**
 * EventStore Adapter for Doctrine
 */
class DoctrineEventStoreAdapter implements Adapter, CanHandleTransaction
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
    protected $streamTableMap = [];

    /**
     * Serialize adapter used to serialize event payload
     *
     * @var string|\Zend\Serializer\Adapter\AdapterInterface
     */
    protected $serializerAdapter;

    /**
     * @var array
     */
    protected $standardColumns = ['event_id', 'event_name', 'event_class', 'created_at', 'payload', 'version'];

    /**
     * @param  array $configuration
     * @throws \Prooph\EventStore\Adapter\Exception\ConfigurationException
     */
    public function __construct(array $configuration)
    {
        if (!isset($configuration['connection'])) {
            throw new ConfigurationException('DB connection configuration is missing');
        }

        if (isset($configuration['stream_table_map'])) {
            $this->streamTableMap = $configuration['stream_table_map'];
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
     * @param Stream $stream
     * @throws \Prooph\EventStore\Exception\RuntimeException If creation of stream fails
     * @return void
     */
    public function create(Stream $stream)
    {
        if (count($stream->streamEvents()) === 0) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $stream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $firstEvent = $stream->streamEvents()[0];

        $this->createSchemaFor($stream->streamName(), $firstEvent->metadata());

        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    /**
     * @param StreamName $streamName
     * @param DomainEvent[] $streamEvents
     * @throws \Prooph\EventStore\Exception\StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $streamName, array $streamEvents)
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
        $events = $this->loadEventsByMetadataFrom($streamName, [], $minVersion);

        return new Stream($streamName, $events);
    }

    /**
     * @param StreamName $streamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return DomainEvent[]
     */
    public function loadEventsByMetadataFrom(StreamName $streamName, array $metadata, $minVersion = null)
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

        if (!is_null($minVersion)) {
            $queryBuilder
                ->andWhere('version >= :version')
                ->setParameter('version', $minVersion);
        }

        /* @var $stmt \Doctrine\DBAL\Statement */
        $stmt = $queryBuilder->execute();

        $events = [];

        foreach ($stmt->fetchAll(\PDO::FETCH_ASSOC) as $eventData) {
            $payload = Serializer::unserialize($eventData['payload'], $this->serializerAdapter);

            $eventClass = $eventData['event_class'];

            //Add metadata stored in table
            foreach ($eventData as $key => $value) {
                if (! in_array($key, $this->standardColumns)) {
                    $metadata[$key] = $value;
                }
            }

            $events[] = $eventClass::fromArray(
                [
                    'uuid' => $eventData['event_id'],
                    'name' => $eventData['event_name'],
                    'version' => (int)$eventData['version'],
                    'created_at' => $eventData['created_at'],
                    'payload' => $payload,
                    'metadata' => $metadata
                ]
            );
        }

        return $events;
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

    public static function addToSchema(Schema $schema, $table, array $metadata)
    {
        $table = $schema->createTable($table);

        $table->addColumn('event_id', 'string', ['length' => 36]);

        $table->addColumn('version', 'integer');
        $table->addColumn('event_name', 'string', ['length' => 100]);
        $table->addColumn('event_class', 'string', ['length' => 100]);
        $table->addColumn('payload', 'text');
        $table->addColumn('created_at', 'string', ['length' => 50]);

        foreach ($metadata as $key => $value) {
            $table->addColumn($key, 'string', ['length' => 100]);
        }

        $table->setPrimaryKey(['event_id']);
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
     * @param StreamName $streamName
     * @param DomainEvent $e
     * @return void
     */
    protected function insertEvent(StreamName $streamName, DomainEvent $e)
    {
        $eventData = [
            'event_id' => $e->uuid()->toString(),
            'version' => $e->version(),
            'event_name' => $e->messageName(),
            'event_class' => get_class($e),
            'payload' => Serializer::serialize($e->payload(), $this->serializerAdapter),
            'created_at' => $e->createdAt()->format(\DateTime::ISO8601)
        ];

        foreach ($e->metadata() as $key => $value) {
            $eventData[$key] = (string)$value;
        }

        $this->connection->insert($this->getTable($streamName), $eventData);
    }

    /**
     * Get table name for given stream name
     *
     * @param StreamName $streamName
     * @return string
     */
    protected function getTable(StreamName $streamName)
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
     * @param StreamName $streamName
     * @return string
     */
    protected function getShortStreamName(StreamName $streamName)
    {
        $streamName = str_replace('-', '_', $streamName->toString());
        return implode('', array_slice(explode('\\', $streamName), -1));
    }
}
