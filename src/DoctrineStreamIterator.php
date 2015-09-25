<?php
/*
 * This file is part of the prooph/event-store-mongodb-adapter.
 * (c) 2014 - 2015 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Date: 09/25/15 - 15:29
 */

namespace Prooph\EventStore\Adapter\Doctrine;

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
    private $currentKey = -1;

    /**
     * @param PDOStatement $statement
     * @param MessageFactory $messageFactory
     * @param PayloadSerializer $payloadSerializer
     * @param array $metadata
     */
    public function __construct(
        PDOStatement $statement,
        MessageFactory $messageFactory,
        PayloadSerializer $payloadSerializer,
        array $metadata
    ) {
        $this->statement = $statement;
        $this->messageFactory = $messageFactory;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadata = $metadata;

        $this->next();
    }

    /**
     * @return null|Message
     */
    public function current()
    {
        if (false === $this->currentItem) {
            return;
        }

        $payload = $this->payloadSerializer->unserializePayload($this->currentItem['payload']);

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
            'metadata' => $this->metadata
        ]);
    }

    /**
     * Next
     */
    public function next()
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

    /**
     * @return bool
     */
    public function valid()
    {
        return false !== $this->currentItem;
    }

    /**
     * Rewind (does nothing)
     */
    public function rewind()
    {
        // do nothing
    }
}
