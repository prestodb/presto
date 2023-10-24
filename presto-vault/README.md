# Vault Secrets Manager Plugin for Presto

This is an implementation for the Secrets Manager Plugin of Presto using the Hashicorp Vault.

### Setting Secrets Manager Properties

Update the etc/secrets-manager.properties, with below configuration.

    secrets-manager.name=vault

Set the below environment variables for the Vault Secrets Manager Plugin.

    export VAULT_ADDR={Vault URL}
    export VAULT_TOKEN={Root token to access Vault}
    export VAULT_SECRET_KEYS={Comma seperated list of catalog names, for which secrets manager is enabled}

### Configuring Secrets

In the sample configuration above, we are specifying a list of secret keys aka catalog names for which secrets are to be fetched. The secrets for each catalog needs to be configured in the Vault as a Key-Value secret and the property secrets-manager.enabled should be set to true in the catalog property file.

#### etc/catalog/[connector].properties

    secretsManager.enabled=true
    [Other connector specific properties if any]

Test execution:

To execute unit tests completely, you need to set up the environment variables 'VAULT_ADDR', 'VAULT_TOKEN', 'VAULT_SECRET_KEYS'. If these are not present the tests will be partially skipped.
