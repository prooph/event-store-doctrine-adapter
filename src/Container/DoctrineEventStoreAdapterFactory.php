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
use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresMandatoryOptions;
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
final class DoctrineEventStoreAdapterFactory implements RequiresConfig, RequiresMandatoryOptions, ProvidesDefaultOptions
{
    use ConfigurationTrait;

    /**
     * @inheritdoc
     */
    public function vendorName()
    {
        return 'prooph';
    }

    /**
     * @inheritdoc
     */
    public function packageName()
    {
        return 'event_store';
    }

    /**
     * @inheritdoc
     */
    public function mandatoryOptions()
    {
        return ['adapter' => ['options']];
    }

    /**
     * @inheritdoc
     */
    public function defaultOptions()
    {
        return ['adapter' => ['options' => ['stream_table_map' => []]]];
    }

    /**
     * @param ContainerInterface $container
     * @return DoctrineEventStoreAdapter
     * @throws \Prooph\EventStore\Exception\ConfigurationException
     */
    public function __invoke(ContainerInterface $container)
    {
        $config = $container->get('config');
        $config = $this->options($config)['adapter']['options'];

        $connection = null;

        if (isset($config['connection_alias']) && $container->has($config['connection_alias'])) {
            $connection = $container->get($config['connection_alias']);
        } elseif (isset($config['connection']) && is_array($config['connection'])) {
            $connection = DriverManager::getConnection($config['connection']);
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

        return new DoctrineEventStoreAdapter(
            $connection,
            $messageFactory,
            $messageConverter,
            $payloadSerializer,
            $config['stream_table_map']
        );
    }
}
