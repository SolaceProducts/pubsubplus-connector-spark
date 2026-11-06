---
type: concept
title: Authentication
description: How the connector authenticates to the Solace broker - basic, client certificate, OAuth client credentials or token file - and how credentials are kept out of notebooks.
status: draft
tags:
  - connector
  - spark
  - authentication
  - oauth
sources:
  - id: docs-config
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Configuring-Connection-Details.htm
    title: Connector for Apache Spark - Configuring connection details
  - id: docs-troubleshooting
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Troubleshooting.htm
    title: Connector for Apache Spark - Troubleshooting
  - id: docs-migration
    resource: https://docs.solace.com/Micro-Integrations/Self-Managed/Spark/Spark-Migration-Guide.htm
    title: Connector for Apache Spark - Migrating to the 4.x line
  - id: jira-89636
    resource: https://sol-jira.atlassian.net/browse/DATAGO-89636
    title: "DATAGO-89636: OAuth authentication support"
  - id: jira-97452
    resource: https://sol-jira.atlassian.net/browse/DATAGO-97452
    title: "DATAGO-97452: Move Solace connection to Spark partitions"
  - id: community-oauth
    resource: https://community.solace.com/t/pubsubplus-connector-spark-and-oauth2/4110
    title: "Solace Community: Pubsubplus-connector-spark and OAuth2"
  - id: databricks-secrets
    resource: https://docs.databricks.com/aws/en/security/secrets/
    title: Databricks - Secret management
---

# Authentication

The connector authenticates to the broker the same way for the source and the sink. The scheme is chosen with the Solace API property `solace.apiProperties.AUTHENTICATION_SCHEME`.[^docs-config]

## Methods

| Method | `AUTHENTICATION_SCHEME` | What to set |
|---|---|---|
| Basic | `AUTHENTICATION_SCHEME_BASIC` (default) | `username`, `password` |
| Client certificate | `AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE` | `username`, plus the Solace API TLS properties through the passthrough, for example `solace.apiProperties.SSL_KEY_STORE`, `SSL_KEY_STORE_PASSWORD`, `SSL_TRUST_STORE` |
| OAuth 2.0, client credentials | `AUTHENTICATION_SCHEME_OAUTH2` | `solace.oauth.client.auth-server-url`, `solace.oauth.client.client-id`, `solace.oauth.client.credentials.client-secret`, and optional TLS settings for the token endpoint |
| OAuth 2.0, token from a file | `AUTHENTICATION_SCHEME_OAUTH2` | `solace.oauth.client.access-token` (absolute path to a file with one token) |

Client certificate authentication has no options of its own. It is entirely Solace API configuration. Since 3.1.1 the connector checks `username` and `password` only for basic authentication, so a certificate setup no longer needs a dummy password.[^jira-97452] The exception is `df.write` inside `foreachBatch`, which still requires both. See [Write path](write-path.md).

## OAuth

OAuth was added in 2.1.0 so that the connector can "fetch access token from OAuth Server or from file at regular intervals" and "update Solace Session without any interruptions".[^jira-89636]

- **Client credentials**: the token is fetched when each session is created, then again every `solace.oauth.client.token.refresh.interval` seconds (default 60). Set the interval below the token lifetime.
- **Token file**: the file is read on the same interval. It must contain exactly one token on one line. If the file is not updated in time, the connector retries the connection according to `reconnectRetries` and then stops. Keep the gap between writing the file and reading it short, so the token has not lost much of its lifetime.
- **Token endpoint TLS**: `solace.oauth.client.auth-server.truststore.file`, `.truststore.password`, `.client-certificate.file`, `.ssl.validate-certificate` (default true), `.tls.version` (default `TLSv1.2`), and `solace.oauth.client.token.fetch.timeout` (default 100 ms, the connection timeout of the token request).[^docs-config]
- **A new token is set on the live session without restarting it.** The broker checks the token when the session next connects or reconnects.

## Keeping credentials out of notebooks

On Databricks, store host, username, password and OAuth secrets in a secret scope and read them with `dbutils.secrets.get(...)` in the option values.[^databricks-secrets] Certificates and truststores can be kept in cloud storage that only the cluster can read.

The Unity Catalog Volume checkpoint options also reference a secret scope, but those credentials are for Databricks, not for Solace. See [Running on Databricks](running-on-databricks.md).

## When credentials fail

- Option checks (missing host, VPN, queue, OAuth fields) run when the query starts, before any connection.
- Broker credentials are first used by the driver's session when the query starts, then by each executor's consumer or producer session when it is created. A bad password therefore fails the query at startup, not at the first message.
- On 3.x, an OAuth server error that is not a well-formed OAuth error (for example an HTML error page) is reported as the literal text `null`. 4.x reports the HTTP status and body.[^docs-migration][^docs-troubleshooting]

## Competency questions

After reading this concept, you should be able to answer these. The short answers point to the detail above.

- **Which authentication methods are supported?** Basic, client certificate, and OAuth 2.0 (client credentials, or a token read from a file).
- **How is client certificate authentication configured?** With `solace.apiProperties.AUTHENTICATION_SCHEME=AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE` and the Solace API keystore and truststore properties. There are no dedicated options.
- **Does a refreshed OAuth token need a restart?** No. The token is refreshed every `solace.oauth.client.token.refresh.interval` seconds and set on the live session, which uses it at its next reconnect.
- **Which OAuth grant is supported?** Client credentials only. Other flows need an external process that writes the token to a file.
- **Why does `foreachBatch` reject an OAuth configuration?** The batch write path always requires `username` and `password`. See Write path.
- **Where should credentials live on Databricks?** In a Databricks secret scope, read with `dbutils.secrets.get`.

## The non-obvious part

**OAuth without a custom truststore has caused setup failures.** A community report describes client-id and secret OAuth on Databricks failing with "No Truststore Password Configured" when no TLS settings were given. The workaround in that thread was the token file.[^community-oauth] When OAuth fails at startup, check the truststore settings first.

**Setting the token endpoint's certificate file overwrites the truststore path.** When `solace.oauth.client.auth-server.client-certificate.file` is set, the connector builds a JKS store from it and saves it to the `truststore.file` path, protected by `truststore.password`. That is why the password becomes required. Point `truststore.file` at a writable path, not at a shared truststore.[^docs-config]

**Broker OAuth and Databricks OAuth are separate.** OAuth to the broker is unrelated to the Databricks OAuth used for Unity Catalog Volume checkpoints. A failure in one says nothing about the other.

[^docs-config]: Connector for Apache Spark - Configuring connection details
[^docs-troubleshooting]: Connector for Apache Spark - Troubleshooting
[^docs-migration]: Connector for Apache Spark - Migrating to the 4.x line
[^jira-89636]: DATAGO-89636: OAuth authentication support
[^jira-97452]: DATAGO-97452: Move Solace connection to Spark partitions
[^community-oauth]: "Solace Community: Pubsubplus-connector-spark and OAuth2"
[^databricks-secrets]: Databricks - Secret management
