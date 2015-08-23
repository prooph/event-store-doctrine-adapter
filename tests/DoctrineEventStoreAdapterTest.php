<?php

namespace DotsUnitedProoph\EventStoreTest\Adapter\Doctrine;

use Doctrine\DBAL\DriverManager;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Stream\DomainEventMetadataWriter;
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

        $this->assertEquals($testStream->streamEvents()[0]->uuid()->toString(), $streamEvents[0]->uuid()->toString());
        $this->assertEquals($testStream->streamEvents()[0]->createdAt()->format('Y-m-d\TH:i:s.uO'), $streamEvents[0]->createdAt()->format('Y-m-d\TH:i:s.uO'));
        $this->assertEquals('Prooph\EventStoreTest\Mock\UserCreated', $streamEvents[0]->messageName());
        $this->assertEquals('contact@prooph.de', $streamEvents[0]->payload()['email']);
        $this->assertEquals(1, $streamEvents[0]->version());
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream()
    {
        $this->adapter->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            array('name' => 'John Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

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

        $streamEvent1 = UsernameChanged::with(
            array('name' => 'John Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent1, 'tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            array('name' => 'Jane Doe'),
            2
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent2, 'tag', 'person');

        $this->adapter->appendTo(new StreamName('Prooph\Model\User'), array($streamEvent1, $streamEvent2));

        $stream = $this->adapter->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertEquals('Prooph\Model\User', $stream->streamName()->toString());
        $this->assertEquals(2, count($stream->streamEvents()));
        $this->assertEquals('John Doe', $stream->streamEvents()[0]->payload()['name']);
        $this->assertEquals('Jane Doe', $stream->streamEvents()[1]->payload()['name']);
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
     * @return Stream
     */
    private function getTestStream()
    {
        $streamEvent = UserCreated::with(
            array('name' => 'Max Mustermann', 'email' => 'contact@prooph.de'),
            1
        );

        DomainEventMetadataWriter::setMetadataKey($streamEvent, 'tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), array($streamEvent));
    }
}
