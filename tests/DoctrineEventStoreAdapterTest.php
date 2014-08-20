<?php

namespace DotsUnitedProoph\EventStoreTest\Adapter\Doctrine;

use Doctrine\DBAL\DriverManager;
use DotsUnited\Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventSourcing\Mapping\AggregateChangedEventHydrator;
use Prooph\EventStore\Stream\AggregateType;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamId;
use Prooph\EventStoreTest\Mock\User;
use Prooph\EventStoreTest\TestCase;
use ValueObjects\DateTime\DateTime;

class DoctrineEventStoreAdapterTest extends TestCase
{
    /**
     * @var DoctrineEventStoreAdapter
     */
    protected $adapter;

    protected function setUp()
    {
        $this->adapter = $this->getEventStoreAdapter();
    }

    protected function initEventStoreAdapter()
    {
        $options = array(
            'connection' => array(
                'driver' => 'pdo_sqlite',
                'dbname' => ':memory:'
            )
        );

        $this->eventStoreAdapter = new DoctrineEventStoreAdapter($options);
    }

    /**
     * @test
     */
    public function it_creates_schema_stores_event_stream_and_fetches_the_stream_from_db()
    {
        $this->adapter->createSchema(array('User'));

        $user = new User("Alex");

        $pendingEvents = $user->accessPendingEvents();

        $eventHydrator = new AggregateChangedEventHydrator();

        $streamEvents = $eventHydrator->toStreamEvents($pendingEvents);

        $stream = new Stream(new AggregateType(get_class($user)), new StreamId($user->id()), $streamEvents);

        $this->adapter->addToExistingStream($stream);

        $historyEventStream = $this->adapter->loadStream(new AggregateType(get_class($user)), new StreamId($user->id()));

        $historyEvents = $historyEventStream->streamEvents();

        $this->assertEquals(1, count($historyEvents));

        $userCreatedEvent = $historyEvents[0];

        $this->assertEquals('Prooph\EventStoreTest\Mock\UserCreated', $userCreatedEvent->eventName()->toString());

        $this->assertEquals($pendingEvents[0]->uuid(), $userCreatedEvent->eventId()->toString());
        $this->assertEquals(1, $userCreatedEvent->version());

        $payload = $userCreatedEvent->payload();

        $this->assertEquals($user->name(), $payload['name']);
        $this->assertTrue($pendingEvents[0]->occurredOn()->sameValueAs(DateTime::fromNativeDateTime($userCreatedEvent->occurredOn())));
    }

    /**
     * @test
     */
    public function it_removes_a_stream()
    {
        $this->adapter->createSchema(array('User'));

        $user = new User("Alex");

        $pendingEvents = $user->accessPendingEvents();

        $eventHydrator = new AggregateChangedEventHydrator();

        $streamEvents = $eventHydrator->toStreamEvents($pendingEvents);

        $stream = new Stream(new AggregateType(get_class($user)), new StreamId($user->id()), $streamEvents);

        $this->adapter->addToExistingStream($stream);

        $this->adapter->removeStream(new AggregateType(get_class($user)), new StreamId($user->id()));

        $historyStream = $this->adapter->loadStream(new AggregateType(get_class($user)), new StreamId($user->id()));

        $this->assertEquals(0, count($historyStream->streamEvents()));
    }

    /**
     * @test
     */
    public function it_can_be_constructed_with_existing_db_adapter()
    {
        $connection = DriverManager::getConnection(array(
            'driver' => 'pdo_sqlite',
            'dbname' => ':memory:'
        ));

        $esAdapter = new DoctrineEventStoreAdapter(array('connection' => $connection));

        $this->assertSame($connection, \PHPUnit_Framework_Assert::readAttribute($esAdapter, 'connection'));
    }
}
