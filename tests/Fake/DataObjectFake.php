<?php

namespace Somar\ForagerElasticsearch\Tests\Fake;

use SilverStripe\Dev\TestOnly;
use SilverStripe\Forager\Extensions\SearchServiceExtension;
use SilverStripe\ORM\DataObject;

/**
 * @property string $Title
 * @property int $ShowInSearch
 * @mixin SearchServiceExtension
 */
class DataObjectFake extends DataObject implements TestOnly
{
    private static string $table_name = 'ForagerElasticsearch_DataObjectFake';

    private static array $db = [
        'Title' => 'Varchar',
        'ShowInSearch' => 'Boolean(1)',
        'Sort' => 'Int',
    ];

    private static array $casting = [
        'getDBHTMLText' => 'HTMLText',
    ];

    private static array $extensions = [
        SearchServiceExtension::class,
    ];

    public function canView(mixed $member = null): bool
    {
        return true;
    }

    public function getDBHTMLText(): string
    {
        return "<h1>WHAT ARE WE YELLING ABOUT?</h1> Then a break <br />Then a new line\nand a tab\t";
    }

}
