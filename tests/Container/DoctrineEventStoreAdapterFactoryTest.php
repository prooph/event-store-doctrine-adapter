<?php
/*
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) 2014-2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 8/24/15 - 12:09 AM
 */
namespace ProophTest\EventStore\Adapter\Doctrine\Container;

use Doctrine\DBAL\Connection;
use Interop\Container\ContainerInterface;
use PHPUnit_Framework_TestCase as TestCase;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Doctrine\Container\DoctrineEventStoreAdapterFactory;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer;
use Prooph\EventStore\Stream\StreamName;

/**
 * Class DoctrineEventStoreAdapterFactoryTest
 * @package ProophTest\EventStore\Adapter\Doctrine\Container
 */
final class DoctrineEventStoreAdapterFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_an_adapter_using_configured_connection_alias()
    {
        $connection = $this->prophesize(Connection::class);

        $config['prooph']['event_store']['adapter']['options']['connection_alias'] = 'app_dbal_connection';

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);
        $container->has('app_dbal_connection')->willReturn(true);
        $container->get('app_dbal_connection')->willReturn($connection->reveal());
        $container->has(MessageFactory::class)->willReturn(false);
        $container->has(MessageConverter::class)->willReturn(false);
        $container->has(PayloadSerializer::class)->willReturn(false);

        $factory = new DoctrineEventStoreAdapterFactory();

        $adapter = $factory($container->reveal());

        $this->assertInstanceOf(DoctrineEventStoreAdapter::class, $adapter);
    }

    /**
     * @test
     */
    public function it_creates_an_adapter_using_configured_connection_options()
    {
        $config['prooph']['event_store']['adapter']['options']['connection'] = [
            'driver' => 'pdo_sqlite',
            'dbname' => ':memory:'
        ];

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);
        $container->has('app_dbal_connection')->willReturn(false);
        $container->has(MessageFactory::class)->willReturn(false);
        $container->has(MessageConverter::class)->willReturn(false);
        $container->has(PayloadSerializer::class)->willReturn(false);

        $factory = new DoctrineEventStoreAdapterFactory();

        $adapter = $factory($container->reveal());

        $this->assertInstanceOf(DoctrineEventStoreAdapter::class, $adapter);
    }

    /**
     * @test
     * @expectedException \Interop\Config\Exception\MandatoryOptionNotFoundException
     */
    public function it_throws_exception_if_adapter_options_are_not_available()
    {
        $config['prooph']['event_store']['adapter'] = [];

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);

        $factory = new DoctrineEventStoreAdapterFactory();
        $factory($container->reveal());
    }

    /**
     * @test
     * @expectedException \Prooph\EventStore\Exception\ConfigurationException
     */
    public function it_throws_exception_if_adapter_connection_could_neither_be_located_nor_created()
    {
        $config['prooph']['event_store']['adapter']['options'] = [];

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);

        $factory = new DoctrineEventStoreAdapterFactory();
        $factory($container->reveal());
    }

    /**
     * @test
     */
    public function it_injects_helpers_from_container_if_available()
    {
        $messageFactory = $this->prophesize(MessageFactory::class);
        $messageConverter = $this->prophesize(MessageConverter::class);
        $payloadSerializer = $this->prophesize(PayloadSerializer::class);

        $connection = $this->prophesize(Connection::class);

        $config['prooph']['event_store']['adapter']['options']['connection_alias'] = 'app_dbal_connection';

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);
        $container->has('app_dbal_connection')->willReturn(true);
        $container->get('app_dbal_connection')->willReturn($connection->reveal());
        $container->has(MessageFactory::class)->willReturn(true);
        $container->get(MessageFactory::class)->willReturn($messageFactory->reveal());
        $container->has(MessageConverter::class)->willReturn(true);
        $container->get(MessageConverter::class)->willReturn($messageConverter->reveal());
        $container->has(PayloadSerializer::class)->willReturn(true);
        $container->get(PayloadSerializer::class)->willReturn($payloadSerializer->reveal());

        $factory = new DoctrineEventStoreAdapterFactory();

        $adapter = $factory($container->reveal());

        $this->assertAttributeSame($messageFactory->reveal(), 'messageFactory', $adapter);
        $this->assertAttributeSame($messageConverter->reveal(), 'messageConverter', $adapter);
        $this->assertAttributeSame($payloadSerializer->reveal(), 'payloadSerializer', $adapter);
    }

    /**
     * @test
     */
    public function it_injects_stream_table_map_from_config()
    {
        $connection = $this->prophesize(Connection::class);

        $config['prooph']['event_store']['adapter']['options']['connection_alias'] = 'app_dbal_connection';
        $config['prooph']['event_store']['adapter']['options']['stream_table_map'] = ['A\Stream' => 'to_table'];

        $container = $this->prophesize(ContainerInterface::class);

        $container->has('config')->willReturn(true);
        $container->get('config')->willReturn($config);
        $container->has('app_dbal_connection')->willReturn(true);
        $container->get('app_dbal_connection')->willReturn($connection->reveal());
        $container->has(MessageFactory::class)->willReturn(false);
        $container->has(MessageConverter::class)->willReturn(false);
        $container->has(PayloadSerializer::class)->willReturn(false);

        $factory = new DoctrineEventStoreAdapterFactory();

        $adapter = $factory($container->reveal());

        $this->assertEquals('to_table', $adapter->getTable(new StreamName('A\Stream')));
    }
}
