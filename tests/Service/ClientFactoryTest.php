<?php

namespace Somar\ForagerElasticsearch\Tests\Service;

use Elastic\Elasticsearch\Client;
use Exception;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use SilverStripe\Dev\SapphireTest;
use Somar\ForagerElasticsearch\Service\ClientFactory;

class ClientFactoryTest extends SapphireTest
{

    protected $usesDatabase = false; // phpcs:ignore SlevomatCodingStandard.TypeHints.PropertyTypeHint

    public function testCreateRequiresEndpointOrCloudId(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessageMatches('/ELASTIC_SEARCH_ENDPOINT or ELASTIC_SEARCH_CLOUD_ID/');

        (new ClientFactory())->create('');
    }

    public function testCreateRequiresApiKey(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessageMatches('/ELASTIC_SEARCH_API_KEY/');

        (new ClientFactory())->create(
            '',
            [
                'endpoint' => 'http://localhost:9200',
            ]
        );
    }

    public function testCreateReturnsConfiguredClient(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                [
                    'Content-Type' => 'application/json',
                    'X-Elastic-Product' => 'Elasticsearch',
                ],
                json_encode(['count' => 0])
            ),
        ]);
        $history = [];
        $handler = HandlerStack::create($mock);
        $handler->push(Middleware::history($history));

        $client = (new ClientFactory())->create(
            '',
            [
                'endpoint' => 'http://localhost:9200',
                'api_key' => 'FakeApiKey',
                'http_client' => new GuzzleClient(['handler' => $handler]),
            ]
        );
        $client->setElasticMetaHeader(false);

        $this->assertInstanceOf(Client::class, $client);

        $client->count(['index' => 'content']);

        $this->assertCount(1, $history);
        $request = $history[0]['request'];
        $this->assertSame('ApiKey FakeApiKey', $request->getHeaderLine('Authorization'));
        $this->assertSame('localhost:9200', $request->getUri()->getAuthority());
    }

}
