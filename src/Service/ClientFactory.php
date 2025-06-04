<?php

namespace Somar\ForagerElasticsearch\Service;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Exception;
use SilverStripe\Core\Injector\Factory;

class ClientFactory implements Factory
{
    /**
     * @throws Exception
     */
    public function create($service, array $params = []): Client // phpcs:ignore SlevomatCodingStandard.TypeHints
    {
        $host = $params['endpoint'] ?? null;
        $cloudId = $params['cloud_id'] ?? null;
        $apiId = $params['api_id'] ?? null;
        $apiKey = $params['api_key'] ?? null;
        $httpClient = $params['http_client'] ?? null;

        $builder = ClientBuilder::create();

        if ($host) {
            $builder->setHosts([$host]);
        } elseif ($cloudId) {
            $builder->setElasticCloudId($cloudId);
        } else {
            throw new Exception(sprintf(
                'The %s implementation requires environment variables: ' .
                'ELASTIC_SEARCH_ENDPOINT or ELASTIC_SEARCH_CLOUD_ID',
                Client::class
            ));
        }

        if (!$apiKey) {
            throw new Exception(sprintf(
                'The %s implementation requires environment variables: ' .
                'ELASTIC_SEARCH_API_KEY ',
                Client::class
            ));
        }

        // If only the API key is provided, Elastic assumes its already base64 encoded
        $builder->setApiKey($apiKey, $apiId);

        if ($httpClient) {
            $builder->setHttpClient($httpClient);
        }

        return $builder->build();
    }
}
