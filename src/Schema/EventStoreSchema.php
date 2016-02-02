<?php
/*
 * This file is part of prooph/event-store-doctrine-adapter.
 * (c) 2014-2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 4/4/15 - 9:58 PM
 */
namespace Prooph\EventStore\Adapter\Doctrine\Schema;

use Doctrine\DBAL\Schema\Schema;

/**
 * Class EventStoreSchema
 *
 * Use this helper in a doctrine migrations script to set up the event store schema
 *
 * @package Prooph\Proophessor\Schema
 * @author Alexander Miertsch <kontakt@codeliner.ws>
 */
final class EventStoreSchema
{
    /**
     * Use this method when you work with a single stream strategy
     *
     * @param Schema $schema
     * @param string $streamName Defaults to 'event_stream'
     * @param bool $withCausationColumns Enable causation columns when using prooph/event-store-bus-bridge
     */
    public static function createSingleStream(Schema $schema, $streamName = 'event_stream', $withCausationColumns = false)
    {
        $eventStream = $schema->createTable($streamName);

        // UUID4 of the event
        $eventStream->addColumn('event_id', 'string', ['fixed' => true, 'length' => 36]);
        // Version of the aggregate after event was recorded
        $eventStream->addColumn('version', 'integer', ['unsigned' => true]);
        // Name of the event
        $eventStream->addColumn('event_name', 'string', ['length' => 100]);
        // Event payload
        $eventStream->addColumn('payload', 'text');
        // DateTime ISO8601 + microseconds UTC stored as a string e.g. 2016-02-02T11:45:39.000000
        $eventStream->addColumn('created_at', 'string', ['fixed' => true, 'length' => 26]);
        // UUID4 of linked aggregate
        $eventStream->addColumn('aggregate_id', 'string', ['fixed' => true, 'length' => 36]);
        // Class of the linked aggregate
        $eventStream->addColumn('aggregate_type', 'string', ['length' => 150]);

        if ($withCausationColumns) {
            // UUID4 of the command which caused the event
            $eventStream->addColumn('causation_id', 'string', ['fixed' => true, 'length' => 36]);
            // Name of the command which caused the event
            $eventStream->addColumn('causation_name', 'string', ['length' => 100]);
        }
        $eventStream->setPrimaryKey(['event_id']);
        // Concurrency check on database level
        $eventStream->addUniqueIndex(['aggregate_id', 'aggregate_type', 'version'], $streamName . '_m_v_uix');
    }

    /**
     * Use this method when you work with an aggregate type stream strategy
     *
     * @param Schema $schema
     * @param string $streamName [shortclassname]_stream
     * @param bool $withCausationColumns Enable causation columns when using prooph/event-store-bus-bridge
     */
    public static function createAggregateTypeStream(Schema $schema, $streamName, $withCausationColumns = false)
    {
        $eventStream = $schema->createTable($streamName);

        // UUID4 of the event
        $eventStream->addColumn('event_id', 'string', ['fixed' => true, 'length' => 36]);
        // Version of the aggregate after event was recorded
        $eventStream->addColumn('version', 'integer', ['unsigned' => true]);
        // Name of the event
        $eventStream->addColumn('event_name', 'string', ['length' => 100]);
        // Event payload
        $eventStream->addColumn('payload', 'text');
        // DateTime ISO8601 + microseconds UTC stored as a string e.g. 2016-02-02T11:45:39.000000
        $eventStream->addColumn('created_at', 'string', ['fixed' => true, 'length' => 26]);
        // UUID4 of linked aggregate
        $eventStream->addColumn('aggregate_id', 'string', ['fixed' => true, 'length' => 36]);
        // Class of the linked aggregate
        $eventStream->addColumn('aggregate_type', 'string', ['length' => 150]);

        if ($withCausationColumns) {
            // UUID4 of the command which caused the event
            $eventStream->addColumn('causation_id', 'string', ['fixed' => true, 'length' => 36]);
            // Name of the command which caused the event
            $eventStream->addColumn('causation_name', 'string', ['length' => 100]);
        }
        $eventStream->setPrimaryKey(['event_id']);
        // Concurrency check on database level
        $eventStream->addUniqueIndex(['aggregate_id', 'version'], $streamName . '_m_v_uix');
    }

    /**
     * Drop a stream schema
     *
     * @param Schema $schema
     * @param string $streamName Defaults to 'event_stream'
     */
    public static function dropStream(Schema $schema, $streamName = 'event_stream')
    {
        $schema->dropTable($streamName);
    }
}
