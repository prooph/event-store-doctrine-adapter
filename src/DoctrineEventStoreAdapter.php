<?php

namespace Prooph\EventStore\Adapter\Doctrine;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Schema;
use Prooph\EventStore\Adapter\AdapterInterface;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Adapter\Feature\TransactionFeatureInterface;
use Prooph\EventStore\Exception\RuntimeException;
use Prooph\EventStore\Stream\EventId;
use Prooph\EventStore\Stream\EventName;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamEvent;
use Prooph\EventStore\Stream\StreamName;
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
    protected $streamTableMap = array();

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
     * @param Stream $aStream
     * @return void
     */
    public function create(Stream $aStream)
    {
        $this->createSchemaFor($aStream);

        $this->appendTo($aStream->streamName(), $aStream->streamEvents());
    }

    /**
     * @param StreamName $aStreamName
     * @param array $streamEvents
     * @throws \Prooph\EventStore\Exception\StreamNotFoundException If stream does not exist
     * @return void
     */
    public function appendTo(StreamName $aStreamName, array $streamEvents)
    {
        foreach ($streamEvents as $event) {
            $this->insertEvent($aStreamName, $event);
        }
    }

    /**
     * @param StreamName $aStreamName
     * @param null|int $minVersion
     * @return Stream|null
     */
    public function load(StreamName $aStreamName, $minVersion = null)
    {
        $events = $this->loadEventsByMetadataFrom($aStreamName, array(), $minVersion);

        return new Stream($aStreamName, $events);
    }

    /**
     * @param StreamName $aStreamName
     * @param array $metadata
     * @param null|int $minVersion
     * @return StreamEvent[]
     */
    public function loadEventsByMetadataFrom(StreamName $aStreamName, array $metadata, $minVersion = null)
    {
        $queryBuilder = $this->connection->createQueryBuilder();

        $table = $this->getTable($aStreamName);

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
                ->where('version >= :version')
                ->setParameter('version', $minVersion);
        }

        /* @var $stmt \Doctrine\DBAL\Statement */
        $stmt = $queryBuilder->execute();

        $events = array();

        foreach ($stmt->fetchAll() as $eventData) {
            $payload = Serializer::unserialize($eventData['payload'], $this->serializerAdapter);

            $eventId = new EventId($eventData['eventId']);

            $eventName = new EventName($eventData['eventName']);

            $occurredOn = new \DateTime($eventData['occurredOn']);

            $events[] = new StreamEvent($eventId, $eventName, $payload, (int) $eventData['version'], $occurredOn, $metadata);
        }

        return $events;
    }

    /**
     * @param Stream $aStream
     * @throws \Prooph\EventStore\Exception\RuntimeException
     * @return bool
     */
    protected function createSchemaFor(Stream $aStream)
    {
        if (count($aStream->streamEvents()) === 0) {
            throw new RuntimeException(
                sprintf(
                    "Cannot create empty stream %s. %s requires at least one event to extract metadata information",
                    $aStream->streamName()->toString(),
                    __CLASS__
                )
            );
        }

        $firstEvent = $aStream->streamEvents()[0];

        $schema = $this->connection->getSchemaManager()->createSchema();

        static::addToSchema($schema, $this->getTable($aStream->streamName()), $firstEvent->metadata());

        $sqls = $schema->toSql($this->connection->getDatabasePlatform());

        foreach ($sqls as $sql) {
            $this->connection->executeQuery($sql);
        }
    }

    public static function addToSchema(Schema $schema, $table, array $metadata)
    {
        $table = $schema->createTable($table);

        $table->addColumn('eventId', 'string', array(
            'length' => 200
        ));

        $table->addColumn('version', 'integer');
        $table->addColumn('eventName', 'text');
        $table->addColumn('payload', 'text');
        $table->addColumn('occurredOn', 'text');

        foreach ($metadata as $key => $value) {
            $table->addColumn($key, 'text');
        }

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
     * @param StreamName $streamName
     * @param StreamEvent $e
     * @return void
     */
    protected function insertEvent(StreamName $streamName, StreamEvent $e)
    {
        $eventData = array(
            'eventId' => $e->eventId()->toString(),
            'version' => $e->version(),
            'eventName' => $e->eventName()->toString(),
            'payload' => Serializer::serialize($e->payload(), $this->serializerAdapter),
            'occurredOn' => $e->occurredOn()->format('Y-m-d\TH:i:s.uO')
        );

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
        return join('', array_slice(explode('\\', $streamName->toString()), -1));
    }
}
