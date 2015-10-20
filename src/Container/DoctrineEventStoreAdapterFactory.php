<?php
/*
 * This file is part of the prooph/event-store-doctrine-adapter.
 * (c) 2014-2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Prooph\EventStore\Adapter\Doctrine\Container;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Adapter\PayloadSerializer;
use Prooph\EventStore\Exception\ConfigurationException;

/**
 * Class DoctrineEventStoreAdapterFactory
 *
 * @package Prooph\EventStore\Adapter\Doctrine\Container
 * @author Alexander Miertsch <kontakt@codeliner.ws>
 */
final class DoctrineEventStoreAdapterFactory
{
    public function __invoke(ContainerInterface $container)
    {
        $config = $container->get('config');

        if (!isset($config['prooph']['event_store']['adapter']['options'])) {
            throw ConfigurationException::configurationError(
                'Missing adapter options configuration in prooph event_store configuration'
            );
        }

        $adapterOptions = $config['prooph']['event_store']['adapter']['options'];

        $connection = null;

        if (isset($adapterOptions['connection_alias']) && $container->has($adapterOptions['connection_alias'])) {
            $connection = $container->get($adapterOptions['connection_alias']);
        } elseif (isset($adapterOptions['connection']) && is_array($adapterOptions['connection'])) {
            $connection = DriverManager::getConnection($adapterOptions['connection']);
        }

        if (! $connection instanceof Connection) {
            throw ConfigurationException::configurationError(sprintf(
                '%s was not able to locate or create a valid Doctrine\DBAL\Connection',
                __CLASS__
            ));
        }

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();

        $payloadSerializer = $container->has(PayloadSerializer::class)
            ? $container->get(PayloadSerializer::class)
            : new PayloadSerializer\JsonPayloadSerializer();

        $streamTableMap = isset($adapterOptions['stream_table_map'])
            ? $adapterOptions['stream_table_map']
            : [];

        return new DoctrineEventStoreAdapter($connection, $messageFactory, $messageConverter, $payloadSerializer, $streamTableMap);
    }
}
