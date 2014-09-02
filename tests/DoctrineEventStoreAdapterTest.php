<?php

namespace DotsUnitedProoph\EventStoreTest\Adapter\Doctrine;

use Doctrine\DBAL\DriverManager;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Stream\EventId;
use Prooph\EventStore\Stream\EventName;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamEvent;
use Prooph\EventStore\Stream\StreamName;
use Prooph\EventStoreTest\TestCase;

class DoctrineEventStoreAdapterTest extends TestCase
{
    /**
     * @var DoctrineEventStoreAdapter
     */
    protected $adapter;

    protected function setUp()
    {
        $options = array(
            'connection' => array(
                'driver' => 'pdo_sqlite',
                'dbname' => ':memory:'
            )
        );

        $this->adapter = new DoctrineEventStoreAdapter($options);
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

        $streamEvents = $this->adapter->loadEventsByMetadataFrom(new StreamName('Prooph\Model\User'), array('tag' => 'person'));

        $this->assertEquals(1, count($streamEvents));

        $this->assertEquals($testStream->streamEvents()[0]->eventId()->toString(), $streamEvents[0]->eventId()->toString());
        $this->assertEquals($testStream->streamEvents()[0]->occurredOn()->format('Y-m-d\TH:i:s.uO'), $streamEvents[0]->occurredOn()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('UserCreated', $streamEvents[0]->eventName()->toString());
        $this->assertEquals('contact@prooph.de', $streamEvents[0]->payload()['email']);
        $this->assertEquals(1, $streamEvents[0]->version());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = new StreamEvent(
            EventId::generate(),
            new EventName('UsernameChanged'),
            array('name' => 'John Doe'),
            2,
            new \DateTime(),
            array('tag' => 'person')
        );

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), array($streamEvent));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'));

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
    }

    /**
     * @test
     */
    public function it_loads_events_from_min_version_on()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent1 = new StreamEvent(
            EventId::generate(),
            new EventName('UsernameChanged'),
            array('name' => 'John Doe'),
            2,
            new \DateTime(),
            array('tag' => 'person')
        );

        $streamEvent2 = new StreamEvent(
            EventId::generate(),
            new EventName('EmailChanged'),
            array('email' => 'test@prooph.de'),
            3,
            new \DateTime(),
            array('tag' => 'person')
        );

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), array($streamEvent1, $streamEvent2));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
        $this->assertEquals('UsernameChanged', $stream->streamEvents()[0]->eventName());
        $this->assertEquals('EmailChanged', $stream->streamEvents()[1]->eventName());
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

    /**
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = new StreamEvent(
            EventId::generate(),
            new EventName('UserCreated'),
            array('name' => 'Max Mustermann', 'email' => 'contact@prooph.de'),
            1,
            new \DateTime(),
            array('tag' => 'person')
        );

        return new Stream(new StreamName('Prooph\Model\User'), array($streamEvent));
    }
}
