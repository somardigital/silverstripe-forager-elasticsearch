<?php

namespace Somar\ForagerElasticsearch\Service;

use InvalidArgumentException;
use OpenSearch\Client;
use Psr\Log\LoggerInterface;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\Core\Environment;
use SilverStripe\Core\Injector\Injectable;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Forager\Exception\IndexConfigurationException;
use SilverStripe\Forager\Exception\IndexingServiceException;
use SilverStripe\Forager\Interfaces\BatchDocumentRemovalInterface;
use SilverStripe\Forager\Interfaces\DocumentInterface;
use SilverStripe\Forager\Interfaces\IndexingInterface;
use SilverStripe\Forager\Schema\Field;
use SilverStripe\Forager\Service\DocumentBuilder;
use SilverStripe\Forager\Service\IndexConfiguration;
use SilverStripe\Forager\Service\Traits\ConfigurationAware;

class OpensearchService implements IndexingInterface, BatchDocumentRemovalInterface
{
    use Configurable;
    use ConfigurationAware;
    use Injectable;

    private const DEFAULT_FIELD_TYPE = 'text';

    private Client $client;

    private DocumentBuilder $builder;

    private static bool $variant_is_suffix = true;

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

    /**
     * Settings keys that require the index to be closed before updating.
     * This supports future analysis updates such as synonyms and stop words.
     */
    private static array $settings_requiring_closed_index = [
        'analysis',
    ];

    public function __construct(Client $client, IndexConfiguration $configuration, DocumentBuilder $builder)
    {
        $this->setClient($client);
        $this->setConfiguration($configuration);
        $this->setBuilder($builder);
    }

    public function getExternalURL(): ?string
    {
        return Environment::getEnv('OPENSEARCH_DASHBOARD') ?: null;
    }

    public function getExternalURLDescription(): ?string
    {
        return 'OpenSearch Dashboard';
    }

    public function getDocumentationURL(): ?string
    {
        return 'https://opensearch.org/docs/latest/';
    }

    public function getMaxDocumentSize(): int
    {
        return $this->config()->get('max_document_size');
    }

    public function addDocument(DocumentInterface $document): ?string
    {
        $ids = $this->addDocuments([$document]);

        return array_shift($ids);
    }

    public function addDocuments(array $documents): array
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

    public function removeDocument(DocumentInterface $document): ?string
    {
        $ids = $this->removeDocuments([$document]);

        return array_shift($ids);
    }

    public function removeDocuments(array $documents): array
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

            foreach ($response['items'] as $item) {
                if (isset($item['delete']['error'])) {
                    throw new IndexingServiceException(
                        sprintf('Failed to remove document: %s', $item['delete']['error']['reason'])
                    );
                }

                $processedIds[] = strval($item['delete']['_id']);
            }
        }

        return array_unique($processedIds);
    }

    /**
     * Remove all documents from the provided index using delete-by-query.
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

    public function getDocument(string $id): ?DocumentInterface
    {
        $result = $this->getDocuments([$id]);

        return $result[0] ?? null;
    }

    public function getDocuments(array $ids): array
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

            $results = $response['docs'] ?? [];

            foreach ($results as $data) {
                if (!($data['found'] ?? false)) {
                    continue;
                }

                $document = $this->getBuilder()->fromArray($data);

                if (!$document) {
                    continue;
                }

                $docs[$document->getIdentifier()] = $document;
            }
        }

        return array_values($docs);
    }

    public function listDocuments(string $indexName, ?int $pageSize = null, int $currentPage = 0): array
    {
        $docs = [];

        $params = [
            'index' => $this->environmentizeIndex($indexName),
            'body' => [
                'query' => [
                    'match_all' => new \stdClass(),
                ],
            ],
        ];

        if ($pageSize !== null) {
            $params['size'] = $pageSize;
            $params['from'] = $currentPage * $pageSize;
        }

        $response = $this->getClient()->search($params);
        $results = $response['hits']['hits'] ?? [];

        foreach ($results as $data) {
            $document = $this->getBuilder()->fromArray($data);

            if (!$document) {
                continue;
            }

            $docs[] = $document;
        }

        return $docs;
    }

    public function getDocumentTotal(string $indexName): int
    {
        $response = $this->getClient()->count([
            'index' => $this->environmentizeIndex($indexName),
        ]);

        return (int) ($response['count'] ?? 0);
    }

    public function getIndexSettings(string $indexName): array
    {
        $index = $this->getConfiguration()->getIndexes()[$indexName] ?? null;

        return $index['settings'] ?? [];
    }

    public function configure(): array
    {
        $indices = $this->getClient()->indices();
        $schemas = [];

        foreach (array_keys($this->getConfiguration()->getIndexes()) as $indexName) {
            $this->validateIndex($indexName);

            $envIndex = $this->environmentizeIndex($indexName);
            $this->findOrMakeIndex($envIndex);

            $definedMappings = $this->getMappingsForFields(
                $this->getConfiguration()->getFieldsForIndex($indexName)
            );

            $definedSettings = $this->getIndexSettings($indexName);

            try {
                if (count($definedMappings) > 0) {
                    $indices->putMapping([
                        'index' => $envIndex,
                        'body' => [
                            'properties' => $definedMappings,
                        ],
                    ]);
                }

                if (count($definedSettings) > 0) {
                    $this->applyIndexSettings($envIndex, $definedSettings);
                }
            } catch (\Throwable $e) {
                throw new IndexingServiceException(sprintf(
                    'Failed to update index mapping and settings: %s',
                    $e->getMessage(),
                ));
            }

            $schemas[$indexName] = true;
        }

        return $schemas;
    }

    public function configureIndexMappings(string $indexName): void
    {
        $this->validateIndex($indexName);

        $indices = $this->getClient()->indices();
        $envIndex = $this->environmentizeIndex($indexName);

        $definedMappings = $this->getMappingsForFields(
            $this->getConfiguration()->getFieldsForIndex($indexName)
        );

        if (count($definedMappings) === 0) {
            return;
        }

        try {
            $indices->putMapping([
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
        }
    }

    public function configureIndexSettings(string $indexName): void
    {
        $this->validateIndex($indexName);

        $envIndex = $this->environmentizeIndex($indexName);
        $definedSettings = $this->getIndexSettings($indexName);

        if (count($definedSettings) === 0) {
            return;
        }

        try {
            $this->applyIndexSettings($envIndex, $definedSettings);
        } catch (\Throwable $e) {
            throw new IndexingServiceException(sprintf(
                'Failed to update index settings: %s',
                $e->getMessage(),
            ));
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
                'Invalid field name: %s. Must contain only lowercase alphanumeric characters and underscores.',
                $field
            ));
        }
    }

    public function environmentizeIndex(string $indexName): string
    {
        $variant = IndexConfiguration::singleton()->getIndexVariant();
        $isSuffix = $this->config()->get('variant_is_suffix');

        if ($variant && $isSuffix) {
            return sprintf('%s_%s', $indexName, $variant);
        }

        if ($variant) {
            return sprintf('%s_%s', $variant, $indexName);
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

    public function setClient(Client $client): OpensearchService
    {
        $this->client = $client;

        return $this;
    }

    public function setBuilder(DocumentBuilder $builder): OpensearchService
    {
        $this->builder = $builder;

        return $this;
    }

    private function findOrMakeIndex(string $index): void
    {
        $indices = $this->getClient()->indices();

        if ($indices->exists(['index' => $index])) {
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
        foreach ($fields as $field) {
            $property = [
                'type' => $field->getOption('type') ?? $this->config()->get('default_field_type'),
            ];

            foreach ($validProperties as $propertyName) {
                if ($field->getOption($propertyName) === null) {
                    continue;
                }

                $property[$propertyName] = $field->getOption($propertyName);
            }

            $properties[$field->getSearchFieldName()] = $property;
        }

        return $properties;
    }

    private function applyIndexSettings(string $indexName, array $settings): void
    {
        $indices = $this->getClient()->indices();
        $shouldClose = $this->settingsRequireClosedIndex($settings);

        try {
            if ($shouldClose) {
                $this->closeIndex($indexName);
            }

            $indices->putSettings([
                'index' => $indexName,
                'body' => [
                    'settings' => $settings,
                ],
            ]);
        } finally {
            if ($shouldClose) {
                $this->openIndex($indexName);
            }
        }
    }

    private function settingsRequireClosedIndex(array $settings): bool
    {
        $keys = $this->config()->get('settings_requiring_closed_index') ?? [];

        foreach ($keys as $key) {
            if (array_key_exists($key, $settings)) {
                return true;
            }
        }

        return false;
    }

    private function closeIndex(string $indexName): void
    {
        $this->getClient()->indices()->close(['index' => $indexName]);
    }

    private function openIndex(string $indexName): void
    {
        $this->getClient()->indices()->open(['index' => $indexName]);
    }

    /**
     * @throws IndexConfigurationException
     */
    private function validateIndex(string $index): void
    {
        $validTypes = $this->config()->get('valid_field_types') ?? [];
        $map = [];

        foreach ($this->getConfiguration()->getClassesForIndex($index) as $class) {
            foreach ($this->getConfiguration()->getFieldsForClass($class) as $field) {
                $this->validateField($field->getSearchFieldName());

                $type = $field->getOption('type') ?? $this->config()->get('default_field_type');

                if (!in_array($type, $validTypes, true)) {
                    throw new IndexConfigurationException(sprintf(
                        'Invalid field type: %s',
                        $type
                    ));
                }

                $alreadyDefined = $map[$field->getSearchFieldName()] ?? null;

                if ($alreadyDefined && $alreadyDefined !== $type) {
                    throw new IndexConfigurationException(sprintf(
                        'Field "%s" is defined twice in the same index with differing types.
                        (%s and %s). Consider changing the field name or explicitly defining
                        the type on each usage',
                        $field->getSearchFieldName(),
                        $alreadyDefined,
                        $type
                    ));
                }

                $map[$field->getSearchFieldName()] = $type;
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
                Injector::inst()->get(LoggerInterface::class)->warning(
                    sprintf('No valid indexes found for document %s, skipping...', $document->getIdentifier())
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
