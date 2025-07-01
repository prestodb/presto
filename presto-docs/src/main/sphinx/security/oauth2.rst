========================
Oauth 2.0 Authentication
========================

Presto can be configured to enable frontend OAuth2 authentication over HTTPS for clients such as the CLI, JDBC, and ODBC drivers. OAuth2 provides a secure and flexible way to authenticate users by using an external identity provider (IdP), such as Okta, Auth0, Azure AD, or Google.

OAuth2 authentication in Presto uses the Authorization Code Flow with PKCE and OpenID Connect (OIDC). The Presto coordinator initiates an OAuth2 challenge, and the client completes the flow by obtaining an access token from the identity provider.

Presto Server Configuration
---------------------------

To enable OAuth2 authentication, configuration changes are made **only on the Presto coordinator**. No changes are required on the workers.

Secure Communication
--------------------

Access to the Presto coordinator must be secured with HTTPS. You must configure a valid TLS certificate and keystore on the coordinator. See the `TLS setup guide <https://prestodb.io/docs/current/security/internal-communication.html>`_ for details.

OAuth2 Configuration
--------------------

Below are the key configuration properties for enabling OAuth2 authentication in ``config.properties``:

.. code-block:: properties

    http-server.authentication.type=OAUTH2

    http-server.authentication.oauth2.issuer=https://your-idp.com/oauth2/default
    http-server.authentication.oauth2.client-id=your-client-id
    http-server.authentication.oauth2.client-secret=your-client-secret
    http-server.authentication.oauth2.scopes=openid,email,profile
    http-server.authentication.oauth2.principal-field=sub
    http-server.authentication.oauth2.groups-field=groups
    http-server.authentication.oauth2.challenge-timeout=15m
    http-server.authentication.oauth2.max-clock-skew=1m
    http-server.authentication.oauth2.refresh-tokens=true
    http-server.authentication.oauth2.oidc.discovery=true
    http-server.authentication.oauth2.state-key=your-hmac-secret
    http-server.authentication.oauth2.additional-audiences=your-client-id,another-audience
    http-server.authentication.oauth2.user-mapping.pattern=(.*)

It is worth noting that ``configuration-based-authorizer.role-regex-map.file-path`` must be configured if
authentication type is set to ``OAUTH2``.

TLS Truststore for IdP
----------------------

If your IdP uses a custom or self-signed certificate, import it into the Java truststore on the Presto coordinator:

.. code-block:: bash

    keytool -import \
      -keystore $JAVA_HOME/lib/security/cacerts \
      -trustcacerts \
      -alias idp_cert \
      -file idp_cert.crt

Notes
-----

- **Issuer**: The base URL of your IdP’s OIDC discovery endpoint.
- **Client ID/Secret**: Registered credentials for Presto in your IdP.
- **Scopes**: Must include ``openid``; others like ``email``, ``profile``, or ``groups`` are optional.
- **Principal Field**: The claim in the ID token used as the Presto username.
- **Groups Field**: Optional claim used for role-based access control.
- **State Key**: A secret used to sign the OAuth2 state parameter (HMAC).
- **Refresh Tokens**: Enable if your IdP supports issuing refresh tokens.
- **Callback**: When configuring your IdP the callback URI must be set to ``[presto]/oauth2/callback``
