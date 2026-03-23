# Silverstripe Forager — OpenSearch provider

OpenSearch search provider for [Silverstripe Forager](https://github.com/silverstripeltd/silverstripe-forager).

This module provides the ability to index content into an [OpenSearch](https://opensearch.org) cluster using the
[opensearch-project/opensearch-php](https://github.com/opensearch-project/opensearch-php) client.

This module **does not** provide any method for performing searches — it handles indexing only.

## Installation

`composer require somardigital/silverstripe-forager-opensearch`

## Activating OpenSearch

Authentication mode is selected automatically:

- In `SS_ENVIRONMENT_TYPE=dev`, the client uses basic authentication
- In non-dev environments, the client uses AWS IAM SigV4 authentication

If you want to support overriding this explicitly, pass `auth_type` through Injector config.

General parameters:

```
OPENSEARCH_ENDPOINT="https://my-opensearch-host:9200"   # Required
OPENSEARCH_SSL_VERIFICATION="true"                      # Optional
OPENSEARCH_INDEX_VARIANT="dev"                         # Optional
OPENSEARCH_DASHBOARD="https://my-opensearch-host:5601" # Optional, not used by this module directly
```

### Local (basic auth)

Define these environment variables for basic auth:

```
SS_ENVIRONMENT_TYPE="dev"
OPENSEARCH_ENDPOINT="https://localhost:9200"
OPENSEARCH_USERNAME="admin"
OPENSEARCH_PASSWORD="secret"
OPENSEARCH_SSL_VERIFICATION="false" # optional if using a self-signed local cert
```

### Non-local (AWS IAM SigV4)

Define these environment variables for SigV4 auth:

```
SS_ENVIRONMENT_TYPE="live"
OPENSEARCH_ENDPOINT="https://search-domain.region.es.amazonaws.com"
OPENSEARCH_AWS_REGION="ap-southeast-2"
OPENSEARCH_AWS_SERVICE="es" # optional, defaults to es
OPENSEARCH_AWS_ACCESS_KEY_ID="AKIA..."       # Optional
OPENSEARCH_AWS_SECRET_ACCESS_KEY="..."       # Optional
OPENSEARCH_AWS_SESSION_TOKEN="..."           # Optional
```

`OPENSEARCH_AWS_SERVICE` defaults to `es` when omitted.

For SigV4 authentication, the AWS SDK for PHP must be available. If explicit credentials are not provided, the client falls back to the default AWS credential provider chain.

> **Security note:** Ensure your OpenSearch endpoint is served over HTTPS so that credentials are encrypted in transit.

## Configuring OpenSearch

The most notable configuration surface is the schema, which determines how data is stored in your
OpenSearch index. There are the following field types currently supported:

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

**Note**: Be careful about changing your schema. OpenSearch may need to be fully reindexed if you
change the name of a field. Fields cannot be deleted, so renaming one will leave any previously created fields around.

## Indexing File Content

The [silverstripe/textextraction](https://docs.silverstripe.org/en/6/optional_features/textextraction/) module is the recommended approach for extracting file content for indexing.

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
