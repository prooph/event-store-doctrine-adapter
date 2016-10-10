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

namespace Prooph\EventStore\Adapter\Doctrine\Container;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Interop\Config\ConfigurationTrait;
use Interop\Config\ProvidesDefaultOptions;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Doctrine\DoctrineEventStoreAdapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;
use Prooph\EventStore\Adapter\Exception\InvalidArgumentException;
use Prooph\EventStore\Adapter\PayloadSerializer;

/**
 * Class DoctrineEventStoreAdapterFactory
 *
 * @package Prooph\EventStore\Adapter\Doctrine\Container
 * @author Alexander Miertsch <kontakt@codeliner.ws>
 */
final class DoctrineEventStoreAdapterFactory implements
    ProvidesDefaultOptions,
    RequiresConfig,
    RequiresConfigId
{
    use ConfigurationTrait;

    /**
     * @var string
     */
    private $configId;

    /**
     * Creates a new instance from a specified config, specifically meant to be used as static factory.
     *
     * In case you want to use another config key than provided by the factories, you can add the following factory to
     * your config:
     *
     * <code>
     * <?php
     * return [
     *     'prooph.event_store.service_name.adapter' => [DoctrineEventStoreAdapterFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): DoctrineEventStoreAdapter
    {
        if (! isset($arguments[0]) || ! $arguments[0] instanceof ContainerInterface) {
            throw new InvalidArgumentException(
                sprintf('The first argument must be of type %s', ContainerInterface::class)
            );
        }
        return (new static($name))->__invoke($arguments[0]);
    }

    public function __construct(string $configId = 'default')
    {
        $this->configId = $configId;
    }

    /**
     * @throws ConfigurationException
     */
    public function __invoke(ContainerInterface $container): DoctrineEventStoreAdapter
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId)['adapter']['options'];

        $connection = null;

        if (isset($config['connection_alias']) && $container->has($config['connection_alias'])) {
            $connection = $container->get($config['connection_alias']);
        } elseif (isset($config['connection']) && is_array($config['connection'])) {
            $connection = DriverManager::getConnection($config['connection']);
        }

        if (! $connection instanceof Connection) {
            throw new ConfigurationException(sprintf(
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
            $config['stream_table_map'],
            $config['load_batch_size']
        );
    }

    public function dimensions(): array
    {
        return ['prooph', 'event_store'];
    }

    public function defaultOptions(): array
    {
        return [
            'adapter' => [
                'options' => [
                    'stream_table_map' => [],
                    'load_batch_size' => 10000,
                ],
            ],
        ];
    }
}
