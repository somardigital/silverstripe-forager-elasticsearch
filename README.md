# 🧺 Silverstripe Forager > <img src="https://www.elastic.co/android-chrome-192x192.png" style="height:40px; vertical-align:middle"/> Elasticsearch Search provider

This module provides the ability to index content for an Elasticsearch engine using Elastic's
[Elasticsearch PHP library](https://github.com/elastic/elasticsearch-php).

Elasticsearch provider for [Silverstripe Forager](https://github.com/silverstripeltd/silverstripe-forager).

This module **does not** provide any method for performing searches on your engines - we've added some
[suggestions](#searching) though.

## Installation

`composer require somardigital/silverstripe-forager-elasticsearch`

## Activating Elasticsearch

To start using Elasticsearch, define environment variables containing your API key, endpoint, and
variant.

```
ELASTIC_SEARCH_ENDPOINT="https://abc123.ap-southeast-2.aws.found.io"
ELASTIC_SEARCH_CLOUD_ID="xxx:abc123==" # Alternative to endpoint
ELASTIC_SEARCH_INDEX_PREFIX="dev"
ELASTIC_SEARCH_API_KEY="abc123"
ELASTIC_SEARCH_API_ID="xxx" # Only required if Api Key does not contain Api ID in base64 encodeed string
ELASTIC_SEARCH_DASHBOARD="https://abc123.ap-southeast-2.aws.found.io:9243"
```

## Configuring Elasticsearch

The most notable configuration surface for Elasticsearch is the schema, which determines how data is stored in your
Elasticsearch index (engine). There are the following types of data currently configured in this module:

- `text` (default)
- `alias`
- `binary`
- `boolean`
- `date`
- `float`
- `geo_point`
- `integer`
- `keyword`
- `long`
- `point`
- `object`
- `nested`

The following additional options are available:
- `fields`
- `format`
- `ignore_above`
- `ignore_malformed`
- `index`
- `meta`
- `path`
- `properties`
- `store`
- `term_vector`

You can specify these data types in the `options` node of your fields.

```yaml
SilverStripe\Forager\Service\IndexConfiguration:
  indexes:
    myindex:
      includeClasses:
        SilverStripe\CMS\Model\SiteTree:
          fields:
            title: true
            summary_field:
              property: SummaryField
              options:
                type: text
          settings:

```

**Note**: Be careful about whimsically changing your schema. ElasticSearch may need to be fully reindexed if you
change the name of a field. Fields cannot be deleted so re-naming one will leave any previously created fields around.

## Indexing File Content

The [silverstripe-text-extraction](https://github.com/silverstripe/silverstripe-textextraction) module is the recommended approach for
fetching the content of files to index. Once configured, you can use the getFileContent method on a file to get the content.

```yaml
SilverStripe\Forager\Service\IndexConfiguration:
  indexes:
    myindex:
      includeClasses:
        SilverStripe\CMS\Model\SiteTree:
          fields:
            pdf_example:
              property: PdfExample.FileContent
              options:
                type: text
```

## Additional documentation

Majority of documentation is provided by the Silverstripe Forager module. A couple in particular that might be useful
to you are:

- [Configuration](https://github.com/silverstripeltd/silverstripe-forager/blob/1/docs/en/configuration.md)
- [Customisation](https://github.com/silverstripeltd/silverstripe-forager/blob/1/docs/en/customising.md)

## Credits

This module is based on the
[silverstripe-forager-elastic-enterprise](https://github.com/silverstripeltd/silverstripe-forager-elastic-enterprise) module
