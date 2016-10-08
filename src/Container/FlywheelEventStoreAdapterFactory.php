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

namespace Prooph\EventStore\Adapter\Flywheel\Container;

use Interop\Config\ConfigurationTrait;
use Interop\Config\RequiresConfig;
use Interop\Config\RequiresConfigId;
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Exception\InvalidArgumentException;
use Prooph\EventStore\Adapter\Flywheel\FlywheelEventStoreAdapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;

final class FlywheelEventStoreAdapterFactory implements RequiresConfig, RequiresConfigId, RequiresMandatoryOptions
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
     *     'prooph.event_store.service_name' => [EventStoreFactory::class, 'service_name'],
     * ];
     * </code>
     *
     * @throws InvalidArgumentException
     */
    public static function __callStatic(string $name, array $arguments): FlywheelEventStoreAdapter
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
    public function __invoke(ContainerInterface $container): FlywheelEventStoreAdapter
    {
        $config = $container->get('config');
        $config = $this->options($config, $this->configId)['adapter']['options'];

        if (! is_dir($config['dir'])) {
            throw new ConfigurationException(sprintf(
                '%s was not able to locate %s',
                __CLASS__,
                $config['dir']
            ));
        }

        $messageFactory = $container->has(MessageFactory::class)
            ? $container->get(MessageFactory::class)
            : new FQCNMessageFactory();

        $messageConverter = $container->has(MessageConverter::class)
            ? $container->get(MessageConverter::class)
            : new NoOpMessageConverter();

        return new FlywheelEventStoreAdapter($config['dir'], $messageFactory, $messageConverter);
    }

    public function dimensions(): array
    {
        return ['prooph', 'event_store'];
    }

    public function mandatoryOptions(): array
    {
        return [
            'adapter' => [
                'options' => [
                    'dir'
                ]
            ]
        ];
    }
}
