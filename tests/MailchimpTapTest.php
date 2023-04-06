<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use GuzzleHttp\Client;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\Psr7\Response;

class MailchimpTapTest extends TestCase
{
    public function testHasDesiredMethods()
    {
        $this->assertTrue(method_exists('MailchimpTap', 'test'));
        $this->assertTrue(method_exists('MailchimpTap', 'discover'));
        $this->assertTrue(method_exists('MailchimpTap', 'tap'));
        $this->assertTrue(method_exists('MailchimpTap', 'getTables'));
    }
}
