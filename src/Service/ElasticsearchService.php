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

    private static bool $prefix_is_suffix = true;

    private static int $max_document_size = 102400;

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
    ];

    public function __construct(
        Client $client,
        IndexConfiguration $configuration,
        DocumentBuilder $builder
    ) {
        $this->setClient($client);
        $this->setConfiguration($configuration);
        $this->setBuilder($builder);
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
        $ids = $this->addDocuments($indexSuffix, [$document]);

        return array_shift($ids);
    }

    public function addDocuments(string $indexSuffix, array $documents): array
    {
        $documentMap = $this->getContentMapForDocuments($documents);
        $processedIds = [];

        foreach ($documentMap as $index => $docsToAdd) {
            $envIndex = $this->environmentizeIndex($index);
            $body = [];

            foreach ($docsToAdd as $document) {
                $body[] = [
                    'index' => [
                        '_index' => $envIndex,
                        '_id' => $document['id'],
                    ],
                ];
                $body[] = $document;
            }

            $response = $this->getClient()->bulk([
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
        }

        return array_unique($processedIds);
    }

    public function removeDocument(string $indexSuffix, DocumentInterface $document): ?string
    {
        $ids = $this->removeDocuments($indexSuffix, [$document]);

        return array_shift($ids);
    }

    public function removeDocuments(string $indexSuffix, array $documents): array
    {
        $documentMap = [];
        $processedIds = [];

        foreach ($documents as $document) {
            if (!$document instanceof DocumentInterface) {
                throw new InvalidArgumentException(sprintf(
                    '%s not passed an instance of %s',
                    __FUNCTION__,
                    DocumentInterface::class
                ));
            }

            $indexes = $this->getConfiguration()->getIndexesForDocument($document);

            foreach (array_keys($indexes) as $indexName) {
                if (!isset($documentMap[$indexName])) {
                    $documentMap[$indexName] = [];
                }

                $documentMap[$indexName][] = $document->getIdentifier();
            }
        }

        foreach ($documentMap as $indexName => $idsToRemove) {
            $envIndex = $this->environmentizeIndex($indexName);
            $body = array_map(static function ($id) use ($envIndex) {
                return [
                    'delete' => [
                        '_index' => $envIndex,
                        '_id' => $id,
                    ],
                ];
            }, $idsToRemove);

            $response = $this->getClient()->bulk([
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
        }

        // One document could have existed in multiple indexes, we only care to track it once
        return array_unique($processedIds);
    }

     /**
     * Forcefully remove all documents from the provided index name.
     * Batches the requests to Elastic based upon the configured batch size,
     * beginning at page 1 and continuing until the index is empty.
     *
     * @param string $indexName The index name to remove all documents from
     * @return int The total number of documents removed
     */
    public function removeAllDocuments(string $indexName): int
    {
        $response = $this->getClient()->deleteByQuery([
            'index' => $this->environmentizeIndex($indexName),
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
        $result = $this->getDocuments([$id]);

        return $result[0] ?? null;
    }

    public function getDocuments(string $indexSuffix, array $ids): array
    {
        $docs = [];
        $indexes = $this->getConfiguration()->getIndexes();

        foreach (array_keys($indexes) as $index) {
            $response = $this->getClient()->mget([
                'index' => $this->environmentizeIndex($index),
                'body' => [
                    'ids' => $ids,
                ],
            ]);

            $results = $response['hits']['hits'] ?? null;

            if (!$results) {
                continue;
            }

            foreach ($results as $data) {
                $document = $this->getBuilder()->fromArray($data);

                if (!$document) {
                    continue;
                }

                $docs[$document->getIdentifier()] = $document;
            }
        }

        return array_values($docs);
    }

    public function listDocuments(
        string $indexSuffix,
        ?int $pageSize = null,
        int $currentPage = 0
    ): array {
        $docs = [];
        $params = [
            'index' => $this->environmentizeIndex($indexSuffix),
            'from' => $currentPage,
        ];

        if ($pageSize) {
            $params['size'] = $pageSize;
        }

        $response = $this->getClient()->search($params);
        $results = $response['results'] ?? null;

        if (!$results) {
            return [];
        }

        foreach ($results as $data) {
            $document = $this->getBuilder()->fromArray($data);

            if (!$document) {
                continue;
            }

            $docs[] = $document;
        }

        return $docs;
    }

    public function getDocumentTotal(string $indexSuffix): int
    {
        $response = $this->getClient()->count([
            'index' => $this->environmentizeIndex($indexSuffix),
        ]);

        return $response['count'] ?? null;
    }

    public function clearIndexDocuments(string $indexSuffix, int $batchSize): int
    {
        return $this->removeAllDocuments($this->environmentizeIndex($indexSuffix));
    }

    public function getIndexSettings(string $indexName): array
    {
        $index = $this->getConfiguration()->getIndexConfigurations()[$indexName] ?? null;

        return $index['settings'] ?? [];
    }

    public function configure(): array
    {
        $indicies = $this->getClient()->indices();
        $schemas = [];

        foreach (array_keys($this->getConfiguration()->getIndexConfigurations()) as $indexName) {
            $this->validateIndex($indexName);

            $envIndex = $this->environmentizeIndex($indexName);
            $this->findOrMakeIndex($envIndex);

            $classes = $this->getConfiguration()
                ->getIndexDataForSuffix($indexName)
                ->getClasses();

            $fields = [];

            foreach ($classes as $class) {
                $classConfig = $this->getConfiguration()
                    ->getIndexConfigurationsForClassName($class);

                $fields = array_merge(
                    $fields,
                    $classConfig[$indexName]['includeClasses'][$class]['fields']
                );
            }

            // Fetch the mappings, as it is currently configured in our application
            $definedMappings = $this->getMappingsForFields($fields);

            // Fetch the settings, as it is currently configured in our application
            $definedSettings = $this->getIndexSettings($indexName);

            // Trigger an update to Elastic with our current configured mappings and settings
            try {
                $indicies->close(['index' => $envIndex]);

                if (count($definedMappings) > 0) {
                    $indicies->putMapping([
                        'index' => $envIndex,
                        'body' => [
                            'properties' => $definedMappings,
                        ],
                    ]);
                }

                if (count($definedSettings) > 0) {
                    $indicies->putSettings([
                        'index' => $envIndex,
                        'body' => [
                            'settings' => $definedSettings,
                        ],
                    ]);
                }
            } catch (\Throwable $e) {
                throw new IndexingServiceException(sprintf(
                    'Failed to update index mapping and settings: %s',
                    $e->getMessage(),
                ));
            } finally {
                // Make sure we re-open the index, regardless of the outcome
                $indicies->open(['index' => $envIndex]);
            }

            // Add this updated schema to our tracked schemas
            $schemas[$indexName] = true;
        }

        return $schemas;
    }

    public function configureIndexMappings(string $indexName): void
    {
        $this->validateIndex($indexName);

        $indicies = $this->getClient()->indices();
        $envIndex = $this->environmentizeIndex($indexName);

        // Fetch the mappings, as configured in our application
        $definedMappings = $this->getMappingsForFields(
            $this->getConfiguration()->getFieldsForIndex($indexName)
        );

        if (count($definedMappings) === 0) {
            return;
        }

        // Trigger an update to Elastic with mappings
        try {
            $indicies->close(['index' => $envIndex]);

            $indicies->putMapping([
                'index' => $envIndex,
                'body' => [
                    'properties' => $definedMappings,
                ],
            ]);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to update index mapping: %s',
                $e->getMessage(),
            ));
        } finally {
            // Make sure we re-open the index, regardless of the outcome
            $indicies->open(['index' => $envIndex]);
        }
    }

    public function configureIndexSettings(string $indexName): void
    {
        $this->validateIndex($indexName);

        $indicies = $this->getClient()->indices();
        $envIndex = $this->environmentizeIndex($indexName);

        // Fetch the settings, as configured in our application
        $definedSettings = $this->getIndexSettings($indexName);

        if (count($definedSettings) === 0) {
            return;
        }

        // Trigger an update to Elastic with settings
        try {
            $indicies->close(['index' => $envIndex]);

            $indicies->putSettings([
                'index' => $envIndex,
                'body' => [
                    'settings' => $definedSettings,
                ],
            ]);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to update index settings: %s',
                $e->getMessage(),
            ));
        } finally {
            // Make sure we re-open the index, regardless of the outcome
            $indicies->open(['index' => $envIndex]);
        }
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

    public function environmentizeIndex(string $indexName): string
    {
        $prefix = IndexConfiguration::singleton()->getIndexPrefix();
        $isSuffix = $this->config()->get('prefix_is_suffix');

        if ($prefix && $isSuffix) {
            // Add as suffix to index name
            return sprintf('%s_%s', $indexName, $prefix);
        }

        if ($prefix) {
            // Add as prefix to index name
            return sprintf('%s_%s', $prefix, $indexName);
        }

        return $indexName;
    }

    public function getClient(): Client
    {
        return $this->client;
    }

    public function getBuilder(): DocumentBuilder
    {
        return $this->builder;
    }

    public function setClient(Client $client): ElasticsearchService
    {
        $this->client = $client;

        return $this;
    }

    public function setBuilder(DocumentBuilder $builder): ElasticsearchService
    {
        $this->builder = $builder;

        return $this;
    }

    private function findOrMakeIndex(string $index): void
    {
        $indices = $this->getClient()->indices();

        if ($indices->exists(['index' => $index])->asBool()) {
            return;
        }

        $indices->create(['index' => $index]);
    }

    /**
     * @param Field[] $fields
     */
    private function getMappingsForFields(array $fields): array
    {
        $validProperties = $this->config()->get('valid_field_properties') ?? [];
        $properties = [];

        /** @var Field $field */
        foreach ($fields as $fieldName => $field) {
            $property = [
                'type' => $field['type'] ?? $this->config()->get('default_field_type'),
            ];

            foreach ($validProperties as $propertyName) {
                if (!isset($field[$propertyName])) {
                    continue;
                }

                $property[$propertyName] = $field[$propertyName];
            }

            $properties[$fieldName] = $property;
        }

        return $properties;
    }

    /**
     * @throws IndexConfigurationException
     */
    private function validateIndex(string $index): void
    {
        $validTypes = $this->config()->get('valid_field_types') ?? [];
        $map = [];

        $classes = $this->getConfiguration()
            ->getIndexDataForSuffix($index)
            ->getClasses();

        foreach ($classes as $class) {
            $classConfig = $this->getConfiguration()
                ->getIndexConfigurationsForClassName($class);

            $fields = $classConfig[$index]['includeClasses'][$class]['fields'];

            foreach ($fields as $fieldName => $field) {
                $type = $field['type'] ?? $this->config()->get('default_field_type');

                if (!in_array($type, $validTypes, true)) {
                    throw new IndexConfigurationException(sprintf(
                        'Invalid field type: %s',
                        $type
                    ));
                }

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
     * @param DocumentInterface[] $documents
     */
    private function getContentMapForDocuments(array $documents): array
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
                $fields = $this->getBuilder()->toArray($document);
            } catch (IndexConfigurationException $e) {
                Injector::inst()->get(LoggerInterface::class)->warning(
                    sprintf('Failed to convert document to array: %s', $e->getMessage())
                );

                continue;
            }

            $indexes = $this->getConfiguration()->getIndexesForDocument($document);

            if (!$indexes) {
                Injector::inst()->get(LoggerInterface::class)->warn(
                    sprintf(
                        'No valid indexes found for document %s, skipping...',
                        $document->getIdentifier()
                    )
                );

                continue;
            }

            foreach (array_keys($indexes) as $indexName) {
                if (!isset($documentMap[$indexName])) {
                    $documentMap[$indexName] = [];
                }

                $documentMap[$indexName][] = $fields;
            }
        }

        return $documentMap;
    }
}
