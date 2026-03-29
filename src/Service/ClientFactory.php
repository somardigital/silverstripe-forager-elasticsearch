<?php

namespace Somar\ForagerElasticsearch\Service;

use InvalidArgumentException;
use OpenSearch\Client;
use OpenSearch\ClientBuilder;
use SilverStripe\Core\Injector\Factory;

class ClientFactory implements Factory
{
    public function create($service, array $params = []): Client
    {
        $endpoint = $params['endpoint'] ?? throw new InvalidArgumentException('Missing OpenSearch endpoint.');

        $sslVerification = filter_var($params['ssl_verification'] ?? true, FILTER_VALIDATE_BOOLEAN);

        $builder = ClientBuilder::create()
            ->setHosts([$endpoint])
            ->setSSLVerification($sslVerification);

        // 1. LOCAL DEV: Use Basic Auth if username & password are provided
        if (!empty($params['username']) && !empty($params['password'])) {
            $builder->setBasicAuthentication($params['username'], $params['password']);
        }

        // 2. AWS: Use SigV4 if an AWS Region and Service are provided
        elseif (!empty($params['aws_region']) && !empty($params['aws_service'])) {
            $builder->setSigV4Region($params['aws_region']);
            $builder->setSigV4Service($params['aws_service']);

            // Use the default AWS credential provider chain for SigV4 authentication.
            // Credentials are resolved from environment variables, local AWS profile files,
            // or IAM roles attached to the running environment. In our case, we expect IAM roles.
            // https://opensearch.org/blog/aws-sigv4-support-for-clients/
            $builder->setSigV4CredentialProvider(true);
        }

        else {
            throw new InvalidArgumentException('Provide either Basic Auth (local) or AWS Region (test/uat/production).');
        }

        return $builder->build();
    }
}
