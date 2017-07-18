<?php
/**
 * @link https://github.com/linpax/microphp-queue
 * @copyright Copyright &copy; 2017 Linpax
 * @license https://github.com/linpax/microphp-queue/blob/master/LICENSE
 */

namespace Micro\Queue;


interface AdapterInterface
{
    /**
     * @param string $name
     * @param array $params
     * @param string $stream
     *
     * @return null|mixed
     */
    public function send($name, array $params = [], $stream = 'sync');

    /**
     * @return bool
     */
    public function test();
}