<?php
/**
 * @link https://github.com/linpax/microphp-queue
 * @copyright Copyright &copy; 2017 Linpax
 * @license https://github.com/linpax/microphp-queue/blob/master/LICENSE
 */

namespace Micro\Queue\Adapter;

use Micro\Queue\AdapterInterface;


class Dummy implements AdapterInterface
{
    public function send($name, array $params = [], $stream = 'sync')
    {
        return $stream !== 'sync' ? null : $stream;
    }

    public function test()
    {
        return true;
    }
}