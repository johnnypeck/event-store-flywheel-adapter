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
use Interop\Config\RequiresMandatoryOptions;
use Interop\Container\ContainerInterface;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\MessageConverter;
use Prooph\Common\Messaging\MessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Adapter\Flywheel\FlywheelEventStoreAdapter;
use Prooph\EventStore\Adapter\Exception\ConfigurationException;

final class FlywheelEventStoreAdapterFactory implements RequiresConfig, RequiresMandatoryOptions
{
    use ConfigurationTrait;

    public function dimensions(): array
    {
        return ['prooph', 'event_store'];
    }

    /**
     * {@inheritdoc}
     */
    public function mandatoryOptions()
    {
        return [
            'adapter' => [
                'options' => [
                    'dir'
                ]
            ]
        ];
    }

    /**
     * @throws ConfigurationException
     */
    public function __invoke(ContainerInterface $container): FlywheelEventStoreAdapter
    {
        $config = $container->get('config');
        $config = $this->options($config)['adapter']['options'];

        if (!is_dir($config['dir'])) {
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
}
