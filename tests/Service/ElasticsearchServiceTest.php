<?php

namespace Somar\ForagerElasticsearch\Tests\Service;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionMethod;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Dev\SapphireTest;
use SilverStripe\Forager\DataObject\DataObjectDocument;
use SilverStripe\Forager\Exception\IndexingServiceException;
use SilverStripe\Forager\Extensions\SearchServiceExtension;
use SilverStripe\Forager\Interfaces\IndexingInterface;
use SilverStripe\Forager\Service\DocumentBuilder;
use SilverStripe\Forager\Service\DocumentFetchCreatorRegistry;
use SilverStripe\Forager\Service\IndexConfiguration;
use SilverStripe\Forager\Service\IndexData;
use SilverStripe\Forager\Tests\Fake\IndexConfigurationFake;
use Somar\ForagerElasticsearch\Service\ClientFactory;
use Somar\ForagerElasticsearch\Service\ElasticsearchService;
use Somar\ForagerElasticsearch\Tests\Fake\DataObjectAlternateFake;
use Somar\ForagerElasticsearch\Tests\Fake\DataObjectFake;
use stdClass;

class ElasticsearchServiceTest extends SapphireTest
{
    protected static $fixture_file = 'ElasticsearchServiceTest.yml'; // phpcs:ignore

    /**
     * @phpcsSuppress SlevomatCodingStandard.TypeHints.PropertyTypeHint.MissingNativeTypeHint
     * @var array
     */
    protected static $extra_dataobjects = [
        DataObjectFake::class,
        DataObjectAlternateFake::class,
    ];

    protected MockHandler $mock;

    protected ElasticsearchService $searchService;

    protected array $history = [];

    public function testMaxDocumentSize(): void
    {
        ElasticsearchService::config()->set('max_document_size', 100);

        $this->assertEquals(100, $this->searchService->getMaxDocumentSize());
    }

    #[DataProvider('provideFieldsForValidation')]
    public function testValidateField(string $fieldName, bool $shouldBeValid): void
    {
        if (!$shouldBeValid) {
            $this->expectExceptionMessage('Invalid field name');
        } else {
            $this->expectNotToPerformAssertions();
        }

        $this->searchService->validateField($fieldName);
    }

    public static function provideFieldsForValidation(): array
    {
        return [
            ['title', true],
            ['title_two', true],
            ['title_2', true],
            ['_title', false],
            ['Title_two', false],
            ['title-2', false],
        ];
    }

    public function testGetMappingsForFields(): void
    {
        $expectedMappings = [
            'source_class' => [
                'type' => 'keyword',
            ],
            'record_base_class' => [
                'type' => 'keyword',
            ],
            'record_id' => [
                'type' => 'long',
            ],
            'title' => [
                'type' => 'text',
            ],
            'html_text' => [
                'type' => 'keyword',
                'ignore_above' => 256,
            ],
        ];

        $this->assertEquals(
            $expectedMappings,
            $this->searchService->getIndexConfigurationMappings('content')
        );
    }

    public function testValidateIndexConfiguration(): void
    {
        $this->expectNotToPerformAssertions();

        $this->searchService->validateIndexConfiguration('content');
    }

    public function testValidateIndexConfigurationInvalidType(): void
    {
        $this->expectExceptionMessage('Invalid field type: fail');

        IndexConfiguration::config()->set(
            'indexes',
            [
                'content' => [
                    'includeClasses' => [
                        DataObjectFake::class => [
                            'fields' => [
                                'title' => [
                                    'options' => [
                                        'type' => 'fail',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ]
        );

        $this->searchService->validateIndexConfiguration('content');
    }

    public function testValidateIndexConfigurationIncompatibleFields(): void
    {
        $this->expectExceptionMessage('Field "fail_field" is defined twice in the same index with differing types');

        IndexConfiguration::config()->set(
            'indexes',
            [
                'content' => [
                    'includeClasses' => [
                        DataObjectFake::class => [
                            'fields' => [
                                'fail_field' => [
                                    'options' => [
                                        'type' => 'date',
                                    ],
                                ],
                            ],
                        ],
                        DataObjectAlternateFake::class => [
                            'fields' => [
                                'fail_field' => [
                                    'options' => [
                                        'type' => 'keyword',
                                    ],
                                ],
                            ],
                        ],
                    ],
                ],
            ]
        );

        $this->searchService->validateIndexConfiguration('content');
    }

    public function testGetContentMapForDocuments(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $documentTwo = $this->objFromFixture(DataObjectFake::class, 'two');
        $documentThree = $this->objFromFixture(DataObjectFake::class, 'three');

        $documents = [
            DataObjectDocument::create($documentOne),
            DataObjectDocument::create($documentTwo),
            DataObjectDocument::create($documentThree),
        ];

        $expectedMap = [
            $this->documentSource($documentOne->ID, 'Dataobject one'),
            $this->documentSource($documentThree->ID, 'Dataobject three'),
        ];

        $reflectionMethod = new ReflectionMethod(ElasticsearchService::class, 'getContentMapForDocuments');
        $reflectionMethod->setAccessible(true);

        $indexData = $this->searchService->getConfiguration()->getIndexDataForSuffix('content');
        $indexData->withIndexContext(
            function (IndexData $index) use ($expectedMap, $reflectionMethod, $documents): void {
                $this->assertEquals(
                    $expectedMap,
                    $reflectionMethod->invoke($this->searchService, 'content', $documents)
                );
            }
        );
    }

    public function testConfigureNewIndex(): void
    {
        $this->searchService->getConfiguration()->setIndexPrefix('dev-test');

        $this->mock->append(new Response(404));
        $this->mock->append($this->elasticResponse(['acknowledged' => true]));

        $this->assertEquals(['content' => true], $this->searchService->configure());
        $this->assertEquals(0, $this->mock->count());

        $this->assertSame('HEAD', $this->history[0]['request']->getMethod());
        $this->assertSame('/dev-test-content', $this->history[0]['request']->getUri()->getPath());
        $this->assertSame('PUT', $this->history[1]['request']->getMethod());
        $this->assertSame('/dev-test-content', $this->history[1]['request']->getUri()->getPath());

        $createBody = json_decode((string) $this->history[1]['request']->getBody(), true);
        $this->assertEquals(
            [
                'settings' => [
                    'number_of_replicas' => 0,
                ],
                'mappings' => [
                    'properties' => [
                        'source_class' => ['type' => 'keyword'],
                        'record_base_class' => ['type' => 'keyword'],
                        'record_id' => ['type' => 'long'],
                        'title' => ['type' => 'text'],
                        'html_text' => [
                            'type' => 'keyword',
                            'ignore_above' => 256,
                        ],
                    ],
                ],
            ],
            $createBody
        );
    }

    public function testConfigureExistingIndex(): void
    {
        $this->searchService->getConfiguration()->setIndexPrefix('dev-test');

        $this->mock->append(new Response(200, ['X-Elastic-Product' => 'Elasticsearch']));
        $this->mock->append($this->elasticResponse(['acknowledged' => true]));
        $this->mock->append($this->elasticResponse(['acknowledged' => true]));

        $this->assertEquals(['content' => true], $this->searchService->configure());
        $this->assertEquals(0, $this->mock->count());
        $this->assertCount(3, $this->history);

        $this->assertSame('HEAD', $this->history[0]['request']->getMethod());
        $this->assertSame('/dev-test-content', $this->history[0]['request']->getUri()->getPath());

        $this->assertSame('PUT', $this->history[1]['request']->getMethod());
        $this->assertSame('/dev-test-content/_settings', $this->history[1]['request']->getUri()->getPath());
        $this->assertSame('reopen=true', $this->history[1]['request']->getUri()->getQuery());
        $this->assertEquals(
            [
                'settings' => [
                    'number_of_replicas' => 0,
                ],
            ],
            json_decode((string) $this->history[1]['request']->getBody(), true)
        );

        $this->assertSame('PUT', $this->history[2]['request']->getMethod());
        $this->assertSame('/dev-test-content/_mapping', $this->history[2]['request']->getUri()->getPath());
        $this->assertSame('', $this->history[2]['request']->getUri()->getQuery());
        $this->assertEquals(
            [
                'properties' => [
                    'source_class' => ['type' => 'keyword'],
                    'record_base_class' => ['type' => 'keyword'],
                    'record_id' => ['type' => 'long'],
                    'title' => ['type' => 'text'],
                    'html_text' => [
                        'type' => 'keyword',
                        'ignore_above' => 256,
                    ],
                ],
            ],
            json_decode((string) $this->history[2]['request']->getBody(), true)
        );
    }

    public function testCreateIndexWithNoConfiguredSettings(): void
    {
        IndexConfiguration::config()->set(
            'indexes',
            [
                'content' => [
                    'includeClasses' => [
                        DataObjectFake::class => [
                            'fields' => [
                                'title' => true,
                            ],
                        ],
                    ],
                ],
            ]
        );

        $this->mock->append($this->elasticResponse(['acknowledged' => true]));

        $this->searchService->createIndex('content');

        $this->assertEquals(0, $this->mock->count());

        // Empty settings must serialise as an object, not an empty array, or Elasticsearch rejects the request
        $body = (string) $this->history[0]['request']->getBody();
        $this->assertStringContainsString('"settings":{}', $body);
        $this->assertStringNotContainsString('"settings":[]', $body);
    }

    public function testUpdateIndexSettingsWithNoConfiguredSettings(): void
    {
        IndexConfiguration::config()->set(
            'indexes',
            [
                'content' => [
                    'includeClasses' => [
                        DataObjectFake::class => [
                            'fields' => [
                                'title' => true,
                            ],
                        ],
                    ],
                ],
            ]
        );

        $this->mock->append($this->elasticResponse(['acknowledged' => true]));

        $this->searchService->updateIndexSettings('content');

        $this->assertEquals(0, $this->mock->count());

        // Empty settings must serialise as an object, not an empty array, or Elasticsearch rejects the request
        $body = (string) $this->history[0]['request']->getBody();
        $this->assertStringContainsString('"settings":{}', $body);
        $this->assertStringNotContainsString('"settings":[]', $body);
    }

    #[DataProvider('provideIndexOperations')]
    public function testIndexOperationsWrapClientFailures(string $method, string $expectedPrefix): void
    {
        $this->mock->append(new Response(400, [], '{"error":"boom"}'));

        $this->expectException(IndexingServiceException::class);
        $this->expectExceptionMessageMatches(sprintf('/^%s:/', preg_quote($expectedPrefix, '/')));

        $this->searchService->{$method}('content');
    }

    public static function provideIndexOperations(): array
    {
        return [
            'createIndex' => ['createIndex', 'Failed to create index'],
            'updateIndexSettings' => ['updateIndexSettings', 'Failed to update index settings'],
            'updateIndexMappings' => ['updateIndexMappings', 'Failed to update index mapping'],
        ];
    }

    public function testGetDocumentTotal(): void
    {
        $this->mock->append($this->elasticResponse(['count' => 146]));

        $this->assertEquals(146, $this->searchService->getDocumentTotal('content'));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testListDocuments(): void
    {
        $fakeOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $fakeThree = $this->objFromFixture(DataObjectFake::class, 'three');

        $this->mock->append($this->elasticResponse([
            'hits' => [
                'hits' => [
                    [
                        '_id' => $this->documentIdentifier($fakeOne->ID),
                        '_source' => $this->documentSource($fakeOne->ID, 'Dataobject one'),
                    ],
                    [
                        '_id' => $this->documentIdentifier($fakeThree->ID),
                        '_source' => $this->documentSource($fakeThree->ID, 'Dataobject three'),
                    ],
                ],
            ],
        ]));

        $documents = $this->searchService->listDocuments('content', 10, 2);

        $this->assertCount(2, $documents);
        $this->assertEquals(0, $this->mock->count());
        $this->assertSame('/content/_search', $this->history[0]['request']->getUri()->getPath());
        $this->assertSame('from=10&size=10', $this->history[0]['request']->getUri()->getQuery());
        $this->assertEquals(
            [
                [
                    'title' => 'Dataobject one',
                    'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
                ],
                [
                    'title' => 'Dataobject three',
                    'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
                ],
            ],
            $this->documentsToArray($documents)
        );
    }

    public function testListDocumentsEmpty(): void
    {
        $this->mock->append($this->elasticResponse(['hits' => ['hits' => []]]));

        $this->assertCount(0, $this->searchService->listDocuments('content'));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testGetDocuments(): void
    {
        $fakeOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $fakeThree = $this->objFromFixture(DataObjectFake::class, 'three');
        $idOne = $this->documentIdentifier($fakeOne->ID);
        $idThree = $this->documentIdentifier($fakeThree->ID);

        $this->mock->append($this->elasticResponse([
            'docs' => [
                [
                    '_id' => $idOne,
                    'found' => true,
                    '_source' => $this->documentSource($fakeOne->ID, 'Dataobject one'),
                ],
                [
                    '_id' => $idThree,
                    'found' => true,
                    '_source' => $this->documentSource($fakeThree->ID, 'Dataobject three'),
                ],
                [
                    '_id' => $idThree,
                    'found' => true,
                    '_source' => $this->documentSource($fakeThree->ID, 'Dataobject three'),
                ],
                [
                    '_id' => 'missing',
                    'found' => false,
                ],
            ],
        ]));

        $documents = $this->searchService->getDocuments('content', [$idOne, $idThree, 'missing']);

        $this->assertCount(2, $documents);
        $this->assertEquals(0, $this->mock->count());
        $this->assertEquals(
            [
                [
                    'title' => 'Dataobject one',
                    'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
                ],
                [
                    'title' => 'Dataobject three',
                    'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
                ],
            ],
            $this->documentsToArray($documents)
        );
    }

    public function testGetDocumentsEmpty(): void
    {
        $this->mock->append($this->elasticResponse(['docs' => []]));

        $this->assertCount(0, $this->searchService->getDocuments('content', ['one', 'two']));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testGetDocument(): void
    {
        $fake = $this->objFromFixture(DataObjectFake::class, 'one');
        $id = $this->documentIdentifier($fake->ID);

        $this->mock->append($this->elasticResponse([
            'docs' => [
                [
                    '_id' => $id,
                    'found' => true,
                    '_source' => $this->documentSource($fake->ID, 'Dataobject one'),
                ],
            ],
        ]));

        $document = $this->searchService->getDocument('content', $id);

        $this->assertNotNull($document);
        $this->assertEquals(
            [
                'title' => 'Dataobject one',
                'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
            ],
            $document->toArray()
        );
        $this->assertEquals(0, $this->mock->count());
    }

    public function testGetDocumentEmpty(): void
    {
        $this->mock->append($this->elasticResponse(['docs' => []]));

        $this->assertNull($this->searchService->getDocument('content', 'missing'));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testAddDocuments(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $documentThree = $this->objFromFixture(DataObjectFake::class, 'three');
        $documents = [
            DataObjectDocument::create($documentOne),
            DataObjectDocument::create($documentThree),
        ];

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['index' => ['_id' => 'doc-123']],
                ['index' => ['_id' => 321]],
                ['index' => ['_id' => '321']],
            ],
        ]));

        $state = new stdClass();
        $state->resultIds = [];
        $indexData = $this->searchService->getConfiguration()->getIndexDataForSuffix('content');
        $indexData->withIndexContext(
            function (IndexData $index) use ($state, $documents): void {
                $state->resultIds = $this->searchService->addDocuments('content', $documents);
            }
        );

        $this->assertEqualsCanonicalizing(['doc-123', '321'], $state->resultIds);
        $this->assertEquals(0, $this->mock->count());

        $request = $this->history[0]['request'];
        $this->assertSame('POST', $request->getMethod());
        $this->assertSame('/content/_bulk', $request->getUri()->getPath());

        $bodyLines = explode("\n", trim((string) $request->getBody()));
        $this->assertEquals(
            ['index' => ['_id' => $this->documentIdentifier($documentOne->ID)]],
            json_decode($bodyLines[0], true)
        );
    }

    public function testAddDocumentsThrowsOnItemError(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $documents = [DataObjectDocument::create($documentOne)];

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['index' => ['_id' => 'doc-123']],
                [
                    'index' => [
                        'error' => [
                            'type' => 'mapper_parsing_exception',
                            'reason' => 'failed to parse field [title]',
                        ],
                    ],
                ],
            ],
        ]));

        $this->expectException(IndexingServiceException::class);
        $this->expectExceptionMessage('Failed to index document: failed to parse field [title]');

        $indexData = $this->searchService->getConfiguration()->getIndexDataForSuffix('content');
        $indexData->withIndexContext(
            function (IndexData $index) use ($documents): void {
                $this->searchService->addDocuments('content', $documents);
            }
        );
    }

    public function testAddDocumentsEmpty(): void
    {
        $this->assertEquals([], $this->searchService->addDocuments('content', []));
        $this->assertCount(0, $this->history);
    }

    public function testAddDocument(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $document = DataObjectDocument::create($documentOne);

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['index' => ['_id' => 'doc-123']],
            ],
        ]));

        $state = new stdClass();
        $state->resultId = null;
        $indexData = $this->searchService->getConfiguration()->getIndexDataForSuffix('content');
        $indexData->withIndexContext(
            function (IndexData $index) use ($state, $document): void {
                $state->resultId = $this->searchService->addDocument('content', $document);
            }
        );

        $this->assertEquals('doc-123', $state->resultId);
        $this->assertEquals(0, $this->mock->count());
    }

    public function testRemoveDocuments(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $documentThree = $this->objFromFixture(DataObjectFake::class, 'three');
        $documents = [
            DataObjectDocument::create($documentOne),
            DataObjectDocument::create($documentThree),
        ];

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['delete' => ['_id' => $this->documentIdentifier($documentOne->ID)]],
                ['delete' => ['_id' => 123]],
                ['delete' => ['_id' => '123']],
            ],
        ]));

        $resultIds = $this->searchService->removeDocuments('content', $documents);

        $this->assertEqualsCanonicalizing([$this->documentIdentifier($documentOne->ID), '123'], $resultIds);
        $this->assertEquals(0, $this->mock->count());
    }

    public function testRemoveDocumentsThrowsOnItemError(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $documents = [DataObjectDocument::create($documentOne)];

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['delete' => ['_id' => $this->documentIdentifier($documentOne->ID)]],
                [
                    'delete' => [
                        'error' => [
                            'reason' => 'index_not_found_exception',
                        ],
                    ],
                ],
            ],
        ]));

        $this->expectException(IndexingServiceException::class);
        $this->expectExceptionMessage('Failed to remove document: index_not_found_exception');

        $this->searchService->removeDocuments('content', $documents);
    }

    public function testRemoveDocumentsRejectsNonDocuments(): void
    {
        try {
            $this->searchService->removeDocuments('content', ['not-a-document']);
            $this->fail('Expected an InvalidArgumentException to be thrown');
        } catch (InvalidArgumentException $e) {
            // The message must name the actual method, not the "{closure}" it was thrown from
            $this->assertStringContainsString(
                sprintf('%s::removeDocuments not passed an instance of', ElasticsearchService::class),
                $e->getMessage()
            );
        }

        $this->assertCount(0, $this->history);
    }

    public function testRemoveDocumentsEmpty(): void
    {
        $this->assertEquals([], $this->searchService->removeDocuments('content', []));
        $this->assertCount(0, $this->history);
    }

    public function testRemoveDocument(): void
    {
        $documentOne = $this->objFromFixture(DataObjectFake::class, 'one');
        $document = DataObjectDocument::create($documentOne);
        $expectedId = $this->documentIdentifier($documentOne->ID);

        $this->mock->append($this->elasticResponse([
            'items' => [
                ['delete' => ['_id' => $expectedId]],
            ],
        ]));

        $this->assertEquals($expectedId, $this->searchService->removeDocument('content', $document));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testClearIndexDocuments(): void
    {
        $this->mock->append($this->elasticResponse(['deleted' => 2]));

        $this->assertEquals(2, $this->searchService->clearIndexDocuments('content', 5));
        $this->assertEquals(0, $this->mock->count());
    }

    public function testClearIndexDocumentsMultipleBatches(): void
    {
        $this->mock->append($this->elasticResponse(['deleted' => 5]));
        $this->mock->append($this->elasticResponse(['deleted' => 3]));

        $this->assertEquals(8, $this->searchService->clearIndexDocuments('content', 5));
        $this->assertEquals(0, $this->mock->count());
        $this->assertCount(2, $this->history);

        foreach ($this->history as $entry) {
            $this->assertSame('POST', $entry['request']->getMethod());
            $this->assertSame('/content/_delete_by_query', $entry['request']->getUri()->getPath());
            // match_all must serialise as an object, not an empty array
            $this->assertStringContainsString('"match_all":{}', (string) $entry['request']->getBody());
        }
    }

    public function testClearIndexDocumentsMissingDeletedKey(): void
    {
        $this->mock->append($this->elasticResponse(['timed_out' => false]));

        $this->assertEquals(0, $this->searchService->clearIndexDocuments('content', 5));
        $this->assertEquals(0, $this->mock->count());
        $this->assertCount(1, $this->history);
    }

    protected function setUp(): void
    {
        parent::setUp();

        IndexConfiguration::config()->set(
            'indexes',
            [
                'content' => [
                    'includeClasses' => [
                        DataObjectFake::class => [
                            'fields' => [
                                'title' => true,
                                'html_text' => [
                                    'property' => 'getDBHTMLText',
                                    'options' => [
                                        'type' => 'keyword',
                                        'ignore_above' => 256,
                                    ],
                                ],
                            ],
                        ],
                    ],
                    'settings' => [
                        'number_of_replicas' => 0,
                    ],
                ],
            ]
        );
        IndexConfiguration::config()->set('crawl_page_content', false);

        $indexConfiguration = $this->mockConfig();

        $this->mock = new MockHandler([]);
        $this->history = [];
        $handler = HandlerStack::create($this->mock);
        $handler->push(Middleware::history($this->history));
        $httpClient = new GuzzleClient(['handler' => $handler]);

        $factory = new ClientFactory();
        $client = $factory->create(
            '',
            [
                'endpoint' => 'http://localhost:9200',
                'api_key' => 'FakeApiKey',
                'http_client' => $httpClient,
            ]
        );
        $client->setElasticMetaHeader(false);

        $documentBuilder = DocumentBuilder::create(
            $indexConfiguration,
            Injector::inst()->get(DocumentFetchCreatorRegistry::class)
        );

        $this->searchService = ElasticsearchService::create($client, $indexConfiguration, $documentBuilder);
        Injector::inst()->registerService($this->searchService, IndexingInterface::class);
        SearchServiceExtension::singleton()->setIndexService($this->searchService);
    }

    protected function mockConfig(): IndexConfigurationFake
    {
        Injector::inst()->registerService($config = new IndexConfigurationFake(), IndexConfiguration::class);
        SearchServiceExtension::singleton()->setConfiguration($config);

        return $config;
    }

    private function elasticResponse(array $body): Response
    {
        return new Response(
            200,
            [
                'Content-Type' => 'application/json',
                'X-Elastic-Product' => 'Elasticsearch',
            ],
            json_encode($body)
        );
    }

    private function documentIdentifier(int $id): string
    {
        return strtolower(sprintf('%s_%s', str_replace('\\', '_', DataObjectFake::class), $id));
    }

    private function documentSource(int $id, string $title): array
    {
        return [
            'title' => $title,
            'html_text' => 'WHAT ARE WE YELLING ABOUT? Then a break Then a new line and a tab ',
            'id' => $this->documentIdentifier($id),
            'record_base_class' => DataObjectFake::class,
            'record_id' => $id,
            'source_class' => DataObjectFake::class,
        ];
    }

    private function documentsToArray(array $documents): array
    {
        $result = [];

        foreach ($documents as $document) {
            $result[] = $document->toArray();
        }

        return $result;
    }

}
