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

namespace Prooph\EventStore\Adapter\Doctrine;

use Doctrine\DBAL\Query\QueryBuilder;
use Iterator;
use Doctrine\DBAL\Driver\PDOStatement;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\PayloadSerializer;

/**
 * Class DoctrineStreamIterator
 * @package Prooph\EventStore\Adapter\Doctrine
 */
final class DoctrineStreamIterator implements Iterator
{
    /**
     * @var QueryBuilder
     */
    private $queryBuilder;

    /**
     * @var PDOStatement
     */
    private $statement;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var PayloadSerializer
     */
    private $payloadSerializer;

    /**
     * @var array
     */
    private $metadata;

    /**
     * @var array
     */
    private $standardColumns = ['event_id', 'event_name', 'created_at', 'payload', 'version'];

    /**
     * @var array|false
     */
    private $currentItem;

    /**
     * @var int
     */
    private $currentKey;

    public function __construct(
        QueryBuilder $queryBuilder,
        MessageFactory $messageFactory,
        PayloadSerializer $payloadSerializer,
        array $metadata
    ) {
        $this->queryBuilder = $queryBuilder;
        $this->messageFactory = $messageFactory;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadata = $metadata;

        $this->rewind();
    }

    /**
     * @return null|Message
     */
    public function current(): ?Message
    {
        if (false === $this->currentItem) {
            return null;
        }

        $payload = $this->payloadSerializer->unserializePayload($this->currentItem['payload']);

        $metadata = [];

        //Add metadata stored in table
        foreach ($this->currentItem as $key => $value) {
            if (! in_array($key, $this->standardColumns)) {
                $metadata[$key] = $value;
            }
        }

        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $this->currentItem['created_at'],
            new \DateTimeZone('UTC')
        );

        return $this->messageFactory->createMessageFromArray($this->currentItem['event_name'], [
            'uuid' => $this->currentItem['event_id'],
            'version' => (int) $this->currentItem['version'],
            'created_at' => $createdAt,
            'payload' => $payload,
            'metadata' => $metadata
        ]);
    }

    public function next(): void
    {
        $this->currentItem = $this->statement->fetch();

        if (false !== $this->currentItem) {
            $this->currentKey++;
        } else {
            $this->currentKey = -1;
        }
    }

    /**
     * @return bool|int
     */
    public function key()
    {
        if (-1 === $this->currentKey) {
            return false;
        }

        return $this->currentKey;
    }

    public function valid(): bool
    {
        return false !== $this->currentItem;
    }

    public function rewind(): void
    {
        //Only perform rewind if current item is not the first element
        if ($this->currentKey !== 0) {
            /* @var $stmt \Doctrine\DBAL\Statement */
            $stmt = $this->queryBuilder->execute();
            $stmt->setFetchMode(\PDO::FETCH_ASSOC);

            $this->currentItem = null;
            $this->currentKey = -1;
            $this->statement = $stmt;

            $this->next();
        }
    }
}
