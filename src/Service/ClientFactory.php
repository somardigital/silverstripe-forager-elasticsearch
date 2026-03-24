<?php

namespace Somar\ForagerElasticsearch\Service;

use InvalidArgumentException;
use OpenSearch\Client;
use OpenSearch\ClientBuilder;
use SilverStripe\Core\Environment;
use SilverStripe\Core\Injector\Factory;

class ClientFactory implements Factory
{
    public function create($service, array $params = []): Client // phpcs:ignore SlevomatCodingStandard.TypeHints
    {
        $endpoint = $params['endpoint'] ?? Environment::getEnv('OPENSEARCH_ENDPOINT') ?: null;
        $username = $params['username'] ?? Environment::getEnv('OPENSEARCH_USERNAME') ?: null;
        $password = $params['password'] ?? Environment::getEnv('OPENSEARCH_PASSWORD') ?: null;

        $awsRegion = $params['aws_region'] ?? Environment::getEnv('OPENSEARCH_AWS_REGION') ?: null;
        $awsService = $params['aws_service'] ?? Environment::getEnv('OPENSEARCH_AWS_SERVICE') ?: 'es';
        $awsAccessKeyId = $params['aws_access_key_id'] ?? Environment::getEnv('OPENSEARCH_AWS_ACCESS_KEY_ID') ?: null;
        $awsSecretAccessKey = $params['aws_secret_access_key'] ?? Environment::getEnv('OPENSEARCH_AWS_SECRET_ACCESS_KEY') ?: null;
        $awsSessionToken = $params['aws_session_token'] ?? Environment::getEnv('OPENSEARCH_AWS_SESSION_TOKEN') ?: null;

        $sslVerification = $params['ssl_verification'] ?? Environment::getEnv('OPENSEARCH_SSL_VERIFICATION');

        if ($sslVerification !== null && is_string($sslVerification)) {
            $normalized = filter_var($sslVerification, FILTER_VALIDATE_BOOLEAN, FILTER_NULL_ON_FAILURE);
            if ($normalized !== null) {
                $sslVerification = $normalized;
            }
        }

        if ($sslVerification === null) {
            $sslVerification = strtolower((string) (Environment::getEnv('SS_ENVIRONMENT_TYPE') ?: 'live')) === 'dev'
                ? false
                : true;
        }
        $environmentType = strtolower((string) (Environment::getEnv('SS_ENVIRONMENT_TYPE') ?: 'live'));
        $authType = $params['auth_type'] ?? ($environmentType === 'dev' ? 'basic' : 'sigv4');

        if (!$endpoint) {
            throw new InvalidArgumentException('Missing OpenSearch endpoint.');
        }

        $parsed = parse_url($endpoint);

        if (!$parsed || empty($parsed['host'])) {
            throw new InvalidArgumentException(sprintf(
                'Invalid OpenSearch endpoint "%s". Expected something like http://localhost:9200 or https://localhost:9200',
                $endpoint
            ));
        }

        $scheme = $parsed['scheme'] ?? 'http';
        $host = $parsed['host'];
        $port = $parsed['port'] ?? ($scheme === 'https' ? 443 : 9200);

        $builder = ClientBuilder::create()
            ->setHosts([[
                'host' => $host,
                'port' => $port,
                'scheme' => $scheme,
            ]])
            ->setSSLVerification($sslVerification);

        if ($authType === 'basic') {
            if (!$username || !$password) {
                throw new InvalidArgumentException('Basic auth requires username and password.');
            }

            $builder->setBasicAuthentication($username, $password);
        } elseif ($authType === 'sigv4') {
            if (!$awsRegion) {
                throw new InvalidArgumentException('SigV4 auth requires aws_region.');
            }

            $builder
                ->setSigV4Region($awsRegion)
                ->setSigV4Service($awsService);

            if ($awsAccessKeyId && $awsSecretAccessKey) {
                $credentials = [
                    'key' => $awsAccessKeyId,
                    'secret' => $awsSecretAccessKey,
                ];

                if ($awsSessionToken) {
                    $credentials['token'] = $awsSessionToken;
                }

                $builder->setSigV4CredentialProvider($credentials);
            } else {
                $builder->setSigV4CredentialProvider(true);
            }
        } else {
            throw new InvalidArgumentException(sprintf('Unsupported auth_type "%s".', $authType));
        }

        return $builder->build();
    }
}
