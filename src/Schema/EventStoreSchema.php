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
     */
    public static function createSingleStream(
        Schema $schema,
        string $streamName = 'event_stream',
        bool $withCausationColumns = false
    ): void {
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
     * Use this method when you work with an aggregate type stream strategy
     */
    public static function createAggregateTypeStream(
        Schema $schema,
        string $streamName,
        bool $withCausationColumns = false
    ): void {
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

    public static function dropStream(Schema $schema, string $streamName = 'event_stream'): void
    {
        $schema->dropTable($streamName);
    }
}
