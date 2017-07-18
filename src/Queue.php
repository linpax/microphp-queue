<?php
/**
 * @link https://github.com/linpax/microphp-queue
 * @copyright Copyright &copy; 2017 Linpax
 * @license https://github.com/linpax/microphp-queue/blob/master/LICENSE
 */

namespace Micro\Queue;

/*
[
    'class' => '\Micro\Queue\Queue',
    'arguments' => [
        'servers' => [
            'server1' => [
                'class' => '\Micro\Queue\RawQueue',
                'ip' => '192.168.10.1',
                'user' => 'name',
                'pass' => 'word',
                'table' => 'queue',
            ],
            'server2' => [
                'class' => '\Micro\Queue\RedisQueue',
                'ip' => '192.168.10.2',
                'user' => 'name',
                'pass' => 'word',
                'table' => 'queue',
            ],
            'server3' => [
                'class' => '\Micro\Queue\RedisQueue',
                'ip' => '192.168.10.3',
                'user' => 'name',
                'pass' => 'word',
                'table' => 'queue',
            ],
            'server4' => [
                'class' => '\Micro\Queue\RabbitMqQueue',
                'ip' => '192.168.10.4',
                'user' => 'name',
                'pass' => 'word',
                'table' => 'queue',
            ]
        ],
        'routes' => [
            'pipeline.service' => 'server1',
            'master.*' => [
                'async' => ['server2'],
                'server3'
            ],
            'broadcast.*' => [
                'stream' => ['server4', 'server1'],
                'sync' => 'server2'
            ]
        ]
    ]
]
 */
class Queue
{
    /** Queue servers */
    private $servers = [];
    /** Queue routes */
    private $routes = [];
    /** @var AdapterInterface[] $brokers Started servers */
    private $brokers = [];


    /**
     * Queue constructor.
     *
     * @param array $servers
     * @param array $routes
     * @param array $brokers
     */
    public function __construct(array $servers = [], array $routes = [], array $brokers = [])
    {
        $this->servers = $servers;
        $this->routes = $routes;
        $this->brokers = $brokers;
    }

    /**
     * Get servers list from routing rule
     *
     * @access protected
     *
     * @param array $route Routing rule
     * @param string $type Sending type
     *
     * @return array
     * @throws \Exception
     */
    protected function getServersFromRoute(array $route, $type = '*')
    {
        $servers = [];

        foreach ($route AS $key => $val) {
            if (is_string($val)) {
                $route['*'] = [$val];
                unset($route[$key]);
            }
        }
        if (array_key_exists($type, $route)) {
            $servers += $route[$type];
        }
        if (array_key_exists('*', $route)) {
            $servers += $route['*'];
        }
        if (!$servers) {
            throw new \Exception('Type `'.$type.'` not found into route');
        }

        return $servers;
    }

    /**
     * Get queue broker
     *
     * @param $name
     * @param string $method
     * @return AdapterInterface|false
     * @throws \Exception
     */
    protected function getBroker($name, $method = 'sync')
    {
        $servers = $this->getServersFromRoute($this->getRoute($name), $method);
        $server = null;

        for ($counter = 0, $num = count($servers); $counter < $num; $counter++) {
            $random = mt_rand(0, count($servers) - 1) ?: 0;

            if (!array_key_exists($servers[$random], $this->brokers)) {
                $cls = $this->servers[$servers[$random]];
                $this->brokers[$servers[$random]] = new $cls['class']($cls);
            }

            if ($this->brokers[$servers[$random]]->test()) {
                $server = $servers[$random];
            }
        }
        if (!$server) {
            throw new \Exception('Message not send, random servers is down into `' . $name . '`');
        }

        return $this->brokers[$server];
    }

    /**
     * Get rules from route by pattern
     *
     * @access protected
     *
     * @param string $uri URI for match
     *
     * @return array Rules for URI
     * @throws \Exception
     */
    protected function getRoute($uri)
    {
        $keys = array_keys($this->routes);

        foreach (range(0, count($keys) - 1) as $i) {
            if (preg_match('/' . $keys[$i] . '/', $uri)) {
                if (!is_array($this->routes[$keys[$i]])) {
                    $this->routes[$keys[$i]] = ['*' => $this->routes[$keys[$i]]];
                }

                return $this->routes[$keys[$i]];
            }
        }
        throw new \Exception('Route `' . $uri . '` not found');
    }

    /**
     * @param $name
     * @param array $params
     * @param string $stream
     * @param int $retry
     * @return bool
     */
    private function send($name, array $params = [], $stream = 'sync', $retry = 10)
    {
        $answer = false;

        for ($i = 0; $i < $retry; $i++) {
            $adapter = $this->getBroker($name, $stream);
            $answer = $adapter->send($name, $params, $stream);

            if ($answer !== false) {
                break;
            }
        }

        return $answer;
    }

    /**
     * Sent sync message (wait answer)
     *
     * @param $name
     * @param array $params
     * @param int $retry
     * @return bool
     */
    public function sync($name, array $params = [], $retry = 10)
    {
        return $this->send($name, $params, 'sync', $retry);
    }

    /**
     * Sent async message (not wait answer)
     *
     * @param $name
     * @param array $params
     * @param int $retry
     * @return bool
     */
    public function async($name, array $params = [], $retry = 10)
    {
        return $this->send($name, $params, 'async', $retry);
    }

    /**
     * Set stream message (async into all listeners)
     *
     * @param $name
     * @param array $params
     * @param int $retry
     * @return bool
     */
    public function stream($name, array $params = [], $retry = 10)
    {
        return $this->send($name, $params, 'stream', $retry);
    }
}