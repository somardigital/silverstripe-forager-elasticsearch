<?php

namespace Somar\ForagerElasticsearch\Tests\Fake;

use SilverStripe\Dev\TestOnly;
use SilverStripe\Forager\Extensions\SearchServiceExtension;
use SilverStripe\ORM\DataObject;

/**
 * @property string $Title
 * @mixin SearchServiceExtension
 */
class DataObjectAlternateFake extends DataObject implements TestOnly
{

    private static array $db = [
        'Title' => 'Varchar',
    ];

    private static string $table_name = 'ForagerElasticsearchDataObjectAlternateFake';

    private static array $extensions = [
        SearchServiceExtension::class,
    ];

    public function canView(mixed $member = null): bool
    {
        return true;
    }

}
