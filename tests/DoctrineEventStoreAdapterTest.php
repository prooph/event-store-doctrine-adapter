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

namespace ProophTest\EventStore\Adapter\Doctrine;

use Doctrine\DBAL\DriverManager;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Adapter\Exception\RuntimeException;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;

/**
 * Class DoctrineEventStoreAdapterTest
 * @package ProophTest\EventStore\Adapter\Doctrine
 */
class DoctrineEventStoreAdapterTest extends TestCase
{
    /**
     * @var DoctrineEventStoreAdapter
     */
    protected $adapter;

    protected function setUp(): void
    {
        $connection = [
            'driver' => 'pdo_sqlite',
            'dbname' => ':memory:'
        ];

        $this->adapter = new DoctrineEventStoreAdapter(
            DriverManager::getConnection($connection),
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new JsonPayloadSerializer()
        );
    }

    /**
     * @test
     */
    public function it_creates_a_stream(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvents = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();

        $testEvent = $testStream->streamEvents()->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
        $this->assertEquals(['tag' => 'person'], $event->metadata());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'));

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $count = 0;
        $lastEvent = null;
        foreach ($stream->streamEvents() as $event) {
            $count++;
            $lastEvent = $event;
        }
        $this->assertEquals(2, $count);
        $this->assertInstanceOf(UsernameChanged::class, $lastEvent);
        $messageConverter = new NoOpMessageConverter();

        $streamEventData = $messageConverter->convertToArray($streamEvent);
        $lastEventData = $messageConverter->convertToArray($lastEvent);

        $this->assertEquals($streamEventData, $lastEventData);
    }

    /**
     * @test
     */
    public function it_loads_events_from_min_version_on(): void
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');


        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());

        $event1 = $stream->streamEvents()->current();
        $stream->streamEvents()->next();
        $event2 = $stream->streamEvents()->current();
        $stream->streamEvents()->next();
        $this->assertFalse($stream->streamEvents()->valid());

        $this->assertEquals('John Doe', $event1->payload()['name']);
        $this->assertEquals('Jane Doe', $event2->payload()['name']);
    }

    /**
     * @test
     */
    public function it_replays(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), null, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(2, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $testEvent = $testStream->streamEvents()->current();
        $event = $streamEvents->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());

        $streamEvents->next();
        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_from_specific_date(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        sleep(1);

        $since = new \DateTime('now', new \DateTimeZone('UTC'));

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), $since, ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();
        $streamEvents->rewind();

        $event = $streamEvents->current();

        $this->assertEquals($streamEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($streamEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('ProophTest\EventStore\Mock\UsernameChanged', $event->messageName());
        $this->assertEquals('John Doe', $event->payload()['name']);
        $this->assertEquals(2, $event->version());
    }

    /**
     * @test
     */
    public function it_replays_events_of_two_aggregates_in_a_single_stream_in_correct_order(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        sleep(1);

        $secondUserEvent = UserCreated::with(
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
            1
        );

        $secondUserEvent = $secondUserEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$secondUserEvent]));

        $streamEvents = $this->adapter->replay(new StreamName('Prooph\Model\User'), null, ['tag' => 'person']);


        $replayedPayloads = [];
        foreach ($streamEvents as $event) {
            $replayedPayloads[] = $event->payload();
        }

        $expectedPayloads = [
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            ['name' => 'John Doe'],
            ['name' => 'Jane Doe', 'email' => 'jane@acme.com'],
        ];

        $this->assertEquals($expectedPayloads, $replayedPayloads);
    }

    /**
     * @test
     */
    public function it_throws_exception_when_second_transaction_started(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Transaction already started');

        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
    }

    /**
     * @test
     */
    public function it_throws_exception_when_empty_stream_created(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot create empty stream Prooph\Model\User.');

        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([])));
    }

    /**
     * @test
     */
    public function it_can_return_sql_string_for_schema_creation(): void
    {
        $sqls = $this->adapter->getSqlSchemaFor(new StreamName('Prooph\Model\User'), []);

        $this->assertInternalType('array', $sqls);
        $this->assertArrayHasKey(0, $sqls);
        $this->assertInternalType('string', $sqls[0]);
    }

    /**
     * @test
     */
    public function it_injected_correct_db_connection(): void
    {
        $connectionMock = $this->getMockForAbstractClass('Doctrine\DBAL\Connection', [], '', false);

        $adapter = new DoctrineEventStoreAdapter(
            $connectionMock,
            new FQCNMessageFactory(),
            new NoOpMessageConverter(),
            new JsonPayloadSerializer()
        );

        $this->assertSame($connectionMock, $adapter->getConnection());
    }

    /**
     * @test
     */
    public function it_can_rollback_transaction(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $this->adapter->beginTransaction();

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));

        $this->adapter->rollback();

        $result = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());
    }

    /**
     * @test
     */
    public function it_can_rewind_doctrine_stream_iterator(): void
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $result = $this->adapter->loadEvents(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());

        $result->rewind();
        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());
        $this->assertFalse($result->key());
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_aggregate_id_and_version(): void
    {
        $this->expectException(ConcurrencyException::class);

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('aggregate_id', 'one');
        $streamEvent = $streamEvent->withAddedMetadata('aggregate_type', 'user');

        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent])));

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('aggregate_id', 'one');
        $streamEvent = $streamEvent->withAddedMetadata('aggregate_type', 'user');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_replays_larger_streams_in_chunks(): void
    {
        $streamName = new StreamName('Prooph\Model\User');

        $streamEvents = [];

        for ($i = 1; $i <= 150; $i++) {
            $streamEvents[] = UserCreated::with(
                ['name' => 'Max Mustermann ' . $i, 'email' => 'contact_' . $i . '@prooph.de'],
                $i
            );
        }

        $this->adapter->create(new Stream($streamName, new \ArrayIterator($streamEvents)));

        $replay = $this->adapter->replay($streamName);

        $count = 0;
        foreach ($replay as $event) {
            $count++;
            $this->assertEquals('Max Mustermann ' . $count, $event->payload()['name']);
            $this->assertEquals('contact_' . $count . '@prooph.de', $event->payload()['email']);
            $this->assertEquals($count, $event->version());
        }

        $this->assertEquals(150, $count);
    }

    /**
     * @return Stream
     */
    private function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
