<?php

namespace Somar\ForagerElasticsearch\Extensions;

use DNADesign\Elemental\Models\BaseElement;
use SilverStripe\Core\Config\Config;
use SilverStripe\Forager\Extensions\SearchServiceExtension;
use SilverStripe\Forager\Service\IndexConfiguration;
use SilverStripe\ORM\DataExtension;
use SilverStripe\ORM\DataObject;

class ElementalReindexExtension extends DataExtension
{
    public function onAfterArchive(): void
    {
        $this->queueParentPageReindex();
    }

    public function onAfterUnpublish(): void
    {
        $this->queueParentPageReindex();
    }

    protected function queueParentPageReindex(): void
    {
        /** @var BaseElement $element */
        $element = $this->owner;

        if (!$element->hasMethod('getPage')) {
            return;
        }

        if (!Config::inst()->get(IndexConfiguration::class, 'index_parent_page_of_elements')) {
            return;
        }

        /** @var DataObject|null $parent */
        $parent = $element->getPage();

        if ($parent && $parent->hasExtension(SearchServiceExtension::class)) {
            $parent->addToIndexes();
        }
    }
}
