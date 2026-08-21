package com.dmp.connector.databricks;

import com.dmp.common.json.Json;
import com.dmp.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

/**
 * An authenticated conversation with one Databricks workspace.
 *
 * <p>Separated from the connector so credential handling has one home, and so the difference
 * between a personal access token and a service principal's OAuth token is invisible to the code
 * that drives the statement lifecycle.
 *
 * <p>Tokens are not refreshed on a timer. The workspace answers 401 when one expires, and a single
 * retry on that answer is both simpler and more correct than predicting an expiry the workspace can
 * shorten at any time. A personal access token cannot be refreshed at all, so a 401 against one is
 * reported immediately rather than retried into the same answer.
 */
final class DatabricksSession {

    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private final HttpClient http;
    private final DatabricksConfig config;
    private final String token;
    private final String clientId;
    private final String clientSecret;

    private String accessToken;

    DatabricksSession(DatabricksConfig config, String token, String clientId, String clientSecret) {
        this.config = config;
        this.token = token;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Obtains a bearer token.
     *
     * <p>A personal access token is already one, so this is a no-op for it. OAuth exchanges the
     * service principal's client credentials at the workspace's own OIDC endpoint, which is why the
     * host matters: tokens are per-workspace and one issued elsewhere is refused here.
     */
    void authenticate() {
        if (config.auth() == DatabricksConfig.Auth.TOKEN) {
            if (token == null || token.isBlank()) {
                throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                        "This connector instance authenticates with a personal access token, but no "
                                + "'token' secret is configured. Either set one, or switch "
                                + "authMethod to OAUTH and configure clientId and clientSecret.");
            }
            this.accessToken = token;
            return;
        }

        if (clientId == null || clientSecret == null) {
            throw new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "OAuth authentication needs both 'clientId' and 'clientSecret' — the service "
                            + "principal's application id and an OAuth secret generated for it.");
        }

        String credentials = Base64.getEncoder().encodeToString(
                (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(config.host() + "/oidc/v1/token"))
                .timeout(TIMEOUT)
                .header("Authorization", "Basic " + credentials)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(
                        form("grant_type", "client_credentials", "scope", "all-apis"))),
                "authenticate");

        if (response.statusCode() != 200) {
            // The body carries the workspace's own error, which distinguishes a wrong secret from a
            // service principal that has not been granted access to this workspace at all.
            throw new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Databricks refused the OAuth token request at " + config.host() + ": "
                            + truncate(response.body()));
        }

        this.accessToken = parse(response.body()).path("access_token").asText(null);

        if (accessToken == null) {
            throw new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Databricks returned no access token: " + truncate(response.body()));
        }
    }

    JsonNode getJson(String url, String what) {
        return parse(expectOk(request(url).GET(), what).body());
    }

    JsonNode postJson(String url, String json, String what) {
        return parse(expectOk(request(url)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8)),
                what).body());
    }

    /**
     * Fetches a pre-signed result file from cloud storage.
     *
     * <p>Deliberately without the workspace credential. The link already carries its own signature,
     * and S3 in particular rejects a request that presents both — a bearer header here produces an
     * opaque 400 that looks like a corrupt link.
     */
    JsonNode getExternal(String url, String what) {
        HttpResponse<String> response = send(HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .GET(), what);

        if (response.statusCode() >= 300) {
            // Worth its own message. These links expire — fifteen minutes is typical — so a 403
            // here usually means the chunk was resolved long before it was read, not that anything
            // is misconfigured. This connector resolves links immediately before reading for
            // exactly that reason, so if it happens the window is genuinely too tight.
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not download a result file from cloud storage while trying to " + what
                            + " (HTTP " + response.statusCode() + "). Pre-signed result links "
                            + "expire, so this usually means the download was attempted too long "
                            + "after the link was issued: " + truncate(response.body()));
        }
        return parse(response.body());
    }

    private HttpRequest.Builder request(String url) {
        if (accessToken == null) {
            authenticate();
        }
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + accessToken);
    }

    /**
     * Sends, retrying once after re-authenticating on a 401.
     *
     * <p>One retry, and only where a fresh token is actually obtainable. Re-running the token
     * exchange is worth a try when a short-lived OAuth token has expired mid-run; repeating a
     * personal access token that was just refused only produces the same refusal.
     */
    private HttpResponse<String> expectOk(HttpRequest.Builder builder, String what) {
        HttpResponse<String> response = send(builder, what);

        if (response.statusCode() == 401 && config.auth() == DatabricksConfig.Auth.OAUTH) {
            authenticate();
            response = send(builder.header("Authorization", "Bearer " + accessToken), what);
        }
        if (response.statusCode() >= 300) {
            throw translate(response, what);
        }
        return response;
    }

    /**
     * Turns an HTTP failure into a classification the engine can act on.
     *
     * <p>The distinction that matters is retryable versus not. A 503 or a warehouse still starting
     * is worth waiting out; a query referencing a column that does not exist will fail identically
     * for ever, and retrying it five times only delays the message that would have told somebody
     * what to fix.
     */
    private ConnectorException translate(HttpResponse<String> response, String what) {
        String detail = truncate(response.body());
        int status = response.statusCode();

        if (status == 401 || status == 403) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Databricks refused to " + what + ": " + detail);
        }
        if (status == 429) {
            return new ConnectorException(ConnectorException.Kind.RATE_LIMITED,
                    "Databricks is throttling this workspace while trying to " + what + ": " + detail);
        }
        if (status == 400 || status == 404 || status == 422) {
            return new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Databricks rejected the request to " + what + ": " + detail);
        }
        return new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                "Databricks could not " + what + " (HTTP " + status + "): " + detail);
    }

    private HttpResponse<String> send(HttpRequest.Builder builder, String what) {
        try {
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not reach Databricks to " + what + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Interrupted while waiting for Databricks to " + what, e);
        }
    }

    private static JsonNode parse(String body) {
        try {
            return Json.mapper().readTree(body);
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Databricks returned a response that is not JSON: " + truncate(body), e);
        }
    }

    private static String form(String... pairs) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < pairs.length; i += 2) {
            if (pairs[i + 1] == null) {
                continue;
            }
            if (!body.isEmpty()) {
                body.append('&');
            }
            body.append(URLEncoder.encode(pairs[i], StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(pairs[i + 1], StandardCharsets.UTF_8));
        }
        return body.toString();
    }

    /** Bounded, because an error body can be a full HTML login page. */
    private static String truncate(String body) {
        if (body == null) {
            return "";
        }
        String trimmed = body.strip();
        return trimmed.length() <= 500 ? trimmed : trimmed.substring(0, 500) + "…";
    }
}
