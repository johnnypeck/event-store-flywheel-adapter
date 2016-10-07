<?php
/**
 * This file is part of the prooph/event-store-flywheel-adapter.
 * (c) 2014-2016 prooph software GmbH <contact@prooph.de>
 * (c) 2015-2016 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Adapter\Flywheel;

use ArrayIterator;
use DateTimeInterface;
use Iterator;
use JamesMoss\Flywheel\Config;
use JamesMoss\Flywheel\Document;
use JamesMoss\Flywheel\Repository;
use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageDataAssertion;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\EventStore\Adapter\Adapter;
use Prooph\EventStore\Stream\Stream;
use Prooph\EventStore\Stream\StreamName;

final class FlywheelEventStoreAdapter implements Adapter
{
    /**
     * @var string
     */
    private $rootDir;

    /**
     * @var MessageFactory
     */
    private $messageFactory;

    /**
     * @var MessageConverter
     */
    private $messageConverter;

    public function __construct(string $rootDir, MessageFactory $messageFactory, MessageConverter $messageConverter)
    {
        $this->rootDir = $rootDir;
        $this->messageFactory = $messageFactory;
        $this->messageConverter = $messageConverter;
    }

    public function create(Stream $stream): void
    {
        $this->appendTo($stream->streamName(), $stream->streamEvents());
    }

    public function appendTo(StreamName $streamName, Iterator $streamEvents): void
    {
        foreach ($streamEvents as $event) {
            $this->insertEvent($streamName, $event);
        }
    }

    public function load(StreamName $streamName, int $minVersion = null): ?Stream
    {
        $events = $this->loadEvents($streamName, [], $minVersion);

        if (! $events->count()) {
            return null;
        }

        return new Stream($streamName, $events);
    }

    public function loadEvents(StreamName $streamName, array $metadata = [], int $minVersion = null): Iterator
    {
        $repository = $this->getRepositoryForStream($streamName);

        $query = $repository->query();

        // Filter by metadata
        foreach ($metadata as $key => $value) {
            $query->where("metadata.$key", '==', $value);
        }

        // Filter by version
        if (null !== $minVersion) {
            $query->where('version', '>=', $minVersion);
        }

        $documents = $query->orderBy('version ASC')->execute();

        $events = [];
        foreach ($documents as $document) {
            $events[] = $this->convertDocumentToEvent($document);
        }

        return new ArrayIterator($events);
    }

    public function replay(StreamName $streamName, DateTimeInterface $since = null, array $metadata = []): Iterator
    {
        $repository = $this->getRepositoryForStream($streamName);

        $query = $repository->query();

        // Filter by metadata
        foreach ($metadata as $key => $value) {
            $query->where("metadata.$key", '==', $value);
        }

        // Filter by creation date
        if (null !== $since) {
            $query->where('created_at', '>=', $since->format('Y-m-d\TH:i:s.u'));
        }

        $documents = $query
            ->orderBy(['created_at ASC', 'version ASC'])
            ->execute();

        $events = [];
        foreach ($documents as $document) {
            $events[] = $this->convertDocumentToEvent($document);
        }

        return new ArrayIterator($events);
    }

    private function insertEvent(StreamName $streamName, Message $event): void
    {
        $repository = $this->getRepositoryForStream($streamName);
        $document = $this->convertEventToDocument($event);

        $repository->store($document);
    }

    private function convertEventToDocument(Message $event): Document
    {
        $eventArr = $this->messageConverter->convertToArray($event);

        MessageDataAssertion::assert($eventArr);

        $data = [
            'event_id' => $eventArr['uuid'],
            'version' => $eventArr['version'],
            'event_name' => $eventArr['message_name'],
            'payload' => $eventArr['payload'],
            'metadata' => $eventArr['metadata'],
            'created_at' => $eventArr['created_at']->format('Y-m-d\TH:i:s.u'),
        ];

        $document = new Document($data);
        $document->setId($data['event_id']);

        return $document;
    }

    private function convertDocumentToEvent(Document $document): Message
    {
        $createdAt = \DateTimeImmutable::createFromFormat(
            'Y-m-d\TH:i:s.u',
            $document->created_at,
            new \DateTimeZone('UTC')
        );

        return $this->messageFactory->createMessageFromArray($document->event_name, [
            'uuid' => $document->event_id,
            'version' => (int) $document->version,
            'created_at' => $createdAt,
            'payload' => (array) $document->payload,
            'metadata' => (array) $document->metadata,
        ]);
    }

    private function getRepositoryForStream(StreamName $streamName): Repository
    {
        return new Repository($streamName->toString(), new Config($this->rootDir));
    }
}
