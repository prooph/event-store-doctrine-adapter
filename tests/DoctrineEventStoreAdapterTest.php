<?php

namespace Prooph\EventStoreTest\Adapter\Doctrine;

use Doctrine\DBAL\DriverManager;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer\JsonPayloadSerializer;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStoreTest\Mock\UserCreated;
use Prooph\EventStoreTest\Mock\UsernameChanged;
use Prooph\EventStoreTest\TestCase;

class DoctrineEventStoreAdapterTest extends TestCase
{
    /**
     * @var DoctrineEventStoreAdapter
     */
    protected $adapter;

    protected function setUp()
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
    public function it_creates_a_stream()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $streamEvents = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $count = 0;
        foreach ($streamEvents as $event) {
            $count++;
        }
        $this->assertEquals(1, $count);

        $testStream->streamEvents()->rewind();

        $testEvent = $testStream->streamEvents()->current();

        $this->assertEquals($testEvent->uuid()->toString(), $event->uuid()->toString());
        $this->assertEquals($testEvent->createdAt()->format('Y-m-d\TH:i:s.uO'), $event->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('Prooph\EventStoreTest\Mock\UserCreated', $event->messageName());
        $this->assertEquals('contact@prooph.de', $event->payload()['email']);
        $this->assertEquals(1, $event->version());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream()
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
        foreach ($stream->streamEvents() as $event) {
            $count++;
        }
        $this->assertEquals(2, $count);
    }

    /**
     * @test
     */
    public function it_loads_events_from_min_version_on()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');


        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            2
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
     * @expectedException RuntimeException
     * @expectedExceptionMessage Transaction already started
     */
    public function it_throws_exception_when_second_transaction_started()
    {
        $this->adapter->beginTransaction();
        $this->adapter->beginTransaction();
    }

    /**
     * @test
     * @expectedException Prooph\EventStore\Exception\RuntimeException
     * @expectedExceptionMessage Cannot create empty stream Prooph\Model\User.
     */
    public function it_throws_exception_when_empty_stream_created()
    {
        $this->adapter->create(new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([])));
    }

    /**
     * @test
     */
    public function it_can_return_sql_string_for_schema_creation()
    {
        $sqls = $this->adapter->createSchemaFor(new StreamName('Prooph\Model\User'), [], true);

        $this->assertInternalType('array', $sqls);
        $this->assertArrayHasKey(0, $sqls);
        $this->assertInternalType('string', $sqls[0]);
    }

    /**
     * @test
     */
    public function it_injected_correct_db_connection()
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
    public function it_can_rollback_transaction()
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

        $result = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

        $this->assertNotNull($result->current());
        $this->assertEquals(0, $result->key());
        $result->next();
        $this->assertNull($result->current());
    }

    /**
     * @test
     */
    public function it_can_rewind_doctrine_stream_iterator()
    {
        $testStream = $this->getTestStream();

        $this->adapter->beginTransaction();

        $this->adapter->create($testStream);

        $this->adapter->commit();

        $result = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), ['tag' => 'person']);

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
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new \ArrayIterator([$streamEvent]));
    }
}
