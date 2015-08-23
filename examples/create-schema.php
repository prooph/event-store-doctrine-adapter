<?php
/*
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) Alexander Miertsch <kontakt@codeliner.ws>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 21.10.14 - 20:03
 */

/**
 * This example shows you the basic set up of the ProophEventStore Doctrine adapter and how you can use the adapter
 * to set up your database.
 */

chdir(__DIR__);

include '../vendor/autoload.php';

//Configure the connection
$connectionParams = [
    'dbname' => 'mydb',
    'user' => 'user',
    'password' => 'secret',
    'host' => 'localhost',
    'driver' => 'pdo_mysql',
];

$esAdapter = new \Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter(['connection' => $connectionParams]);

//Pass adapter to event store via configuration
$esConfig = new Prooph\EventStore\Configuration\Configuration();

$esConfig->setAdapter($esAdapter);

$eventStore = new \Prooph\EventStore\EventStore($esConfig);

//When using the \Prooph\EventStore\Stream\SingleStreamStrategy
//you can use the following code snippet to create a database schema:
$eventStore->getAdapter()->createSchemaFor(
    new \Prooph\EventStore\Stream\StreamName('event_stream'),
    ['aggregate_id' => 'string', 'aggregate_type' => 'string']
);

//When using the \Prooph\EventStore\Stream\AggregateTypeStreamStrategy
//you need to create a stream table for each aggregate root class
//The aggregate_type metadata is not required for this strategy
$eventStore->getAdapter()->createSchemaFor(
    new \Prooph\EventStore\Stream\StreamName('My\Model\AggregateRoot'),
    ['aggregate_id' => 'string']
);


//The \Prooph\EventStore\Stream\AggregateStreamStrategy needs no existing tables. It creates a new table for
//each aggregate instance. This strategy is not the best choice when working with a RDBMS,
//cause it will end up in having a lot of tables.
//But it becomes really interesting when using NoSql databases.
