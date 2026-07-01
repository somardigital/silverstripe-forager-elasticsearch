<?php

namespace Somar\ForagerElasticsearch\Service;

use Elastic\Elasticsearch\Client;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Environment;
use SilverStripe\Core\Injector\Injectable;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Forager\Exception\IndexConfigurationException;
use SilverStripe\Forager\Exception\IndexingServiceException;
use SilverStripe\Forager\Interfaces\BatchDocumentInterface;
use SilverStripe\Forager\Interfaces\DocumentInterface;
use SilverStripe\Forager\Interfaces\IndexingInterface;
use SilverStripe\Forager\Schema\Field;
use SilverStripe\Forager\Service\DocumentBuilder;
use SilverStripe\Forager\Service\IndexConfiguration;
use SilverStripe\Forager\Service\Traits\ConfigurationAware;

class ElasticsearchService implements IndexingInterface, BatchDocumentInterface
{

    use Configurable;
    use ConfigurationAware;
    use Injectable;

    private const DEFAULT_FIELD_TYPE = 'text';

    private Client $client;

    private DocumentBuilder $builder;

    private static int $max_document_size = 10485760;

    private static string $default_field_type = self::DEFAULT_FIELD_TYPE;

    private static array $valid_field_types = [
        'alias',
        'binary',
        'boolean',
        'date',
        'float',
        'geo_point',
        'integer',
        'keyword',
        'long',
        'point',
        'object',
        'nested',
        'text',
        'semantic_text',
        'dense_vector',
        'search_as_you_type',
    ];

    private static array $valid_field_properties = [
        'fields',
        'format',
        'ignore_above',
        'ignore_malformed',
        'index',
        'meta',
        'path',
        'properties',
        'store',
        'term_vector',
        'similarity',
        'index_options',
        'analyzer',
        'search_analyzer',
        'normalizer',
        'dims',
        'scaling_factor',
        'inference_id',
        'search_inference_id',
        'chunking_settings',
    ];

    public function __construct(Client $client, IndexConfiguration $configuration, DocumentBuilder $exporter)
    {
        $this->setClient($client);
        $this->setConfiguration($configuration);
        $this->setBuilder($exporter);
    }

    public function getExternalURL(): ?string
    {
        return Environment::getEnv('ELASTIC_SEARCH_DASHBOARD') ?: null;
    }

    public function getExternalURLDescription(): ?string
    {
        return 'Elastic Search Dashboard';
    }

    public function getDocumentationURL(): ?string
    {
        return 'https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html';
    }

    public function getMaxDocumentSize(): int
    {
        return $this->config()->get('max_document_size');
    }

    public function addDocument(string $indexSuffix, DocumentInterface $document): ?string
    {
        $processedIds = $this->addDocuments($indexSuffix, [$document]);

        return array_shift($processedIds);
    }

    /**
     * @throws IndexingServiceException
     * @link https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk
     */
    public function addDocuments(string $indexSuffix, array $documents): array
    {
        $documentsArray = $this->getContentMapForDocuments($indexSuffix, $documents);
        $processedIds = [];

        if (!$documentsArray) {
            return [];
        }

        $body = [];

        foreach ($documentsArray as $document) {
            $body[] = [
                'index' => [
                    '_id' => $document['id'],
                ],
            ];
            $body[] = $document;
        }

        $response = $this->getClient()->bulk([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
            'body' => $body,
        ]);

        // Grab all the ID values, and also cast them to string
        foreach ($response['items'] as $item) {
            if (isset($item['index']['error'])) {
                throw new IndexingServiceException(
                    sprintf('Failed to index document: %s', $item['index']['error']['reason'])
                );
            }

            $processedIds[] = strval($item['index']['_id']);
        }

        return array_unique($processedIds);
    }

    public function removeDocument(string $indexSuffix, DocumentInterface $document): ?string
    {
        $processedIds = $this->removeDocuments($indexSuffix, [$document]);

        return array_shift($processedIds);
    }

    public function removeDocuments(string $indexSuffix, array $documents): array
    {
        $processedIds = [];

        $body = array_map(static function ($document) {
            if (!$document instanceof DocumentInterface) {
                throw new InvalidArgumentException(sprintf(
                    '%s not passed an instance of %s',
                    __FUNCTION__,
                    DocumentInterface::class
                ));
            }

            return [
                'delete' => [
                    '_id' => $document->getIdentifier(),
                ],
            ];
        }, $documents);

        if (!$body) {
            return [];
        }

        $response = $this->getClient()->bulk([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
            'body' => $body,
        ]);

        // Grab all the ID values, and also cast them to string
        foreach ($response['items'] as $item) {
            if (isset($item['delete']['error'])) {
                throw new IndexingServiceException(
                    sprintf('Failed to remove document: %s', $item['delete']['error']['reason'])
                );
            }

            $processedIds[] = strval($item['delete']['_id']);
        }

        // One document could have existed in multiple indexes, we only care to track it once
        return array_unique($processedIds);
    }

    public function clearIndexDocuments(string $indexSuffix, int $batchSize): int
    {
        $response = $this->getClient()->deleteByQuery([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
            'conflicts' => 'proceed',
            'allow_no_indices' => false,
            'body' => [
                'query' => [
                    'match_all' => new \stdClass(),
                ],
            ],
        ]);

        return $response['deleted'] ?? 0;
    }

    public function getDocument(string $indexSuffix, string $id): ?DocumentInterface
    {
        $result = $this->getDocuments($indexSuffix, [$id]);

        return $result[0] ?? null;
    }

    public function getDocuments(string $indexSuffix, array $ids): array
    {
        $docs = [];

        $response = $this->getClient()->mget([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
            'body' => [
                'ids' => $ids,
            ],
        ]);

        $results = $response['docs'] ?? null;

        if (!$results) {
            return [];
        }

        foreach ($results as $data) {
            if (($data['found'] ?? true) === false) {
                continue;
            }

            $document = $this->getBuilder()->fromArray((array) $data['_source']);

            if (!$document) {
                continue;
            }

            // Stored by identifier as the key
            $docs[$document->getIdentifier()] = $document;
        }

        return array_values($docs);
    }

    public function listDocuments(string $indexSuffix, ?int $pageSize = null, int $currentPage = 1): array
    {
        $params = [
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
            'from' => $currentPage - 1,
        ];

        if ($pageSize) {
            $params['size'] = $pageSize;
        }

        $response = $this->getClient()->search($params);
        $results = $response['hits']['hits'] ?? null;

        if (!$results) {
            return [];
        }

        $documents = [];

        foreach ($results as $data) {
            $document = $this->getBuilder()->fromArray((array) $data['_source']);

            if (!$document) {
                continue;
            }

            $documents[] = $document;
        }

        return $documents;
    }

    public function getDocumentTotal(string $indexSuffix): int
    {
        $response = $this->getClient()->count([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
        ]);

        return (int) ($response['count'] ?? 0);
    }

    public function configure(): array
    {
        $schemas = [];

        foreach (array_keys($this->getConfiguration()->getIndexConfigurations()) as $indexSuffix) {
            $this->validateIndexConfiguration($indexSuffix);

            if (!$this->indexExists($indexSuffix)) {
                $this->createIndex($indexSuffix);
            } else {
                $this->updateIndex($indexSuffix);
            }

            // Add this updated schema to our tracked schemas
            $schemas[$indexSuffix] = true;
        }

        return $schemas;
    }

    public function validateField(string $field): void
    {
        if ($field[0] === '_') {
            throw new IndexConfigurationException(sprintf(
                'Invalid field name: %s. Fields cannot begin with underscores.',
                $field
            ));
        }

        if (preg_match('/[^a-z0-9_]/', $field)) {
            throw new IndexConfigurationException(sprintf(
                'Invalid field name: %s. Must contain only lowercase alphanumeric' .
                    'characters and underscores.',
                $field
            ));
        }
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    public function getBuilder(): DocumentBuilder
    {
        return $this->builder;
    }

    public function setClient(Client $client): static
    {
        $this->client = $client;

        return $this;
    }

    public function setBuilder(DocumentBuilder $builder): static
    {
        $this->builder = $builder;

        return $this;
    }

    protected function indexExists(string $indexSuffix): bool
    {
        return $this->getClient()->indices()->exists([
            'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
        ])->asBool();
    }

    protected function createIndex(string $indexSuffix): void
    {
        $definedSettings = $this->getIndexConfigurationSettings($indexSuffix);

        $definedMappings = $this->getIndexConfigurationMappings($indexSuffix);

        try {
            $this->getClient()->indices()->create([
                'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
                'body' => [
                    'settings' => $definedSettings,
                    'mappings' => [
                        'properties' => $definedMappings,
                    ],
                ],
            ]);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to create index: %s',
                $e->getMessage(),
            ));
        }
    }

    protected function updateIndex(string $indexSuffix): void
    {
        $this->updateIndexSettings($indexSuffix);
        $this->updateIndexMappings($indexSuffix);
    }

    protected function updateIndexSettings(string $indexSuffix): void
    {
        $definedSettings = $this->getIndexConfigurationSettings($indexSuffix);

        try {
            $this->getClient()->indices()->putSettings([
                'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
                'body' => [
                    'settings' => $definedSettings,
                ],
            ]);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to update index settings: %s',
                $e->getMessage(),
            ));
        }
    }

    protected function updateIndexMappings(string $indexSuffix): void
    {
        $definedMappings = $this->getIndexConfigurationMappings($indexSuffix);

        try {
            $this->getClient()->indices()->putMapping([
                'index' => $this->getConfiguration()->environmentizeIndex($indexSuffix),
                'body' => [
                    'properties' => $definedMappings,
                ],
            ]);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to update index mapping: %s',
                $e->getMessage(),
            ));
        }
    }

    protected function getIndexConfigurationSettings(string $indexSuffix): array
    {
        $index = $this->getConfiguration()->getIndexConfigurations()[$indexSuffix] ?? null;

        return $index['settings'] ?? [];
    }

    protected function getIndexConfigurationMappings(string $indexSuffix): array
    {
        $fields = $this->getConfiguration()
            ->getIndexDataForSuffix($indexSuffix)
            ->getFields();

        $validProperties = $this->config()->get('valid_field_properties') ?? [];
        $properties = [];

        /** @var Field $field */
        foreach ($fields as $fieldName => $field) {
            $property = [
                'type' => $field->getOption('type') ?? $this->config()->get('default_field_type'),
            ];

            foreach ($validProperties as $propertyName) {
                if ($field->getOption($propertyName) === null) {
                    continue;
                }

                $property[$propertyName] = $field->getOption($propertyName);
            }

            $properties[$fieldName] = $property;
        }

        return $properties;
    }

    /**
     * @throws IndexConfigurationException
     */
    protected function validateIndexConfiguration(string $index): void
    {
        $validTypes = array_filter(array_values($this->config()->get('valid_field_types'))) ?? [];

        $map = [];

        $classes = $this->getConfiguration()
            ->getIndexDataForSuffix($index)
            ->getClasses();

        foreach ($classes as $class) {
            foreach ($this->getConfiguration()->getFieldsForClass($class) as $field) {
                $type = $field->getOption('type') ?? $this->config()->get('default_field_type');

                if (!in_array($type, $validTypes, true)) {
                    throw new IndexConfigurationException(sprintf(
                        'Invalid field type: %s',
                        $type
                    ));
                }

                $fieldName = $field->getSearchFieldName();

                $alreadyDefined = $map[$fieldName] ?? null;

                if ($alreadyDefined && $alreadyDefined !== $type) {
                    throw new IndexConfigurationException(sprintf(
                        'Field "%s" is defined twice in the same index with differing types.
                        (%s and %s). Consider changing the field name or explicitly defining
                        the type on each usage',
                        $fieldName,
                        $alreadyDefined,
                        $type
                    ));
                }

                $map[$fieldName] = $type;
            }
        }
    }

    /**
     * @throws InvalidArgumentException
     * @throws IndexingServiceException
     */
    protected function getContentMapForDocuments(string $indexSuffix, array $documents): array
    {
        $documentMap = [];

        foreach ($documents as $document) {
            if (!$document instanceof DocumentInterface) {
                throw new InvalidArgumentException(sprintf(
                    '%s not passed an instance of %s',
                    __FUNCTION__,
                    DocumentInterface::class
                ));
            }

            if (!$document->shouldIndex()) {
                continue;
            }

            try {
                $documentToArray = $this->getBuilder()->toArray($document);
            } catch (IndexConfigurationException $e) {
                Injector::inst()->get(LoggerInterface::class)->warning(
                    sprintf('Failed to convert document to array: %s', $e->getMessage())
                );

                continue;
            }

            $indexes = $this->getConfiguration()->getIndexConfigurationsForDocument($document);

            if (!$indexes) {
                Injector::inst()->get(LoggerInterface::class)->warning(
                    sprintf('No valid indexes found for document %s, skipping...', $document->getIdentifier())
                );

                continue;
            }

            if (!in_array($indexSuffix, array_keys($indexes), true)) {
                Injector::inst()->get(LoggerInterface::class)->warning(
                    sprintf(
                        '%s is not a valid index for document %s, skipping...',
                        $indexSuffix,
                        $document->getIdentifier()
                    )
                );

                continue;
            }

            $documentMap[] = $documentToArray;
        }

        return $documentMap;
    }
}
