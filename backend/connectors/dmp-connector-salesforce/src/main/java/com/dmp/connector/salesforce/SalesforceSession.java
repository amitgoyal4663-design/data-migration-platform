package com.dmp.connector.salesforce;

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
import java.nio.file.Path;
import java.time.Duration;

/**
 * An authenticated conversation with one Salesforce org.
 *
 * <p>Holds the access token and knows how to talk to the REST API. Separated from the connector so
 * the credential handling has one home rather than being threaded through the job lifecycle, and so
 * a token refresh is one place rather than a check at every call site.
 *
 * <p>Tokens are not refreshed on a timer. Salesforce answers 401 when one expires, and a single
 * retry on that answer is both simpler and more correct than predicting an expiry the org can
 * shorten at any time — a session policy change would silently break a timer and cannot break this.
 */
final class SalesforceSession {

    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private final HttpClient http;
    private final SalesforceConfig config;
    private final String clientId;
    private final String clientSecret;
    private final String username;
    private final String password;

    private String accessToken;
    private String instanceUrl;

    SalesforceSession(SalesforceConfig config, String clientId, String clientSecret,
                      String username, String password) {
        this.config = config;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    /**
     * Obtains a token.
     *
     * <p>Two flows, because orgs differ in what they permit. Client credentials is the right one
     * for a server-to-server integration and needs no user identity at all; the password flow is
     * offered because a great many existing orgs still have it enabled and refusing to work with
     * them would make the connector useless in exactly the environments that need a migration.
     */
    void authenticate() {
        String body = clientSecret != null && !clientSecret.isBlank() && username == null
                ? form("grant_type", "client_credentials", "client_id", clientId,
                        "client_secret", clientSecret)
                : form("grant_type", "password", "client_id", clientId,
                        "client_secret", clientSecret, "username", username,
                        "password", password);

        HttpResponse<String> response = send(HttpRequest.newBuilder(
                        URI.create(config.loginUrl() + "/services/oauth2/token"))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body)), "authenticate");

        if (response.statusCode() != 200) {
            // The body carries Salesforce's own error, which distinguishes a bad secret from a
            // disabled flow from an IP restriction — all of which look identical as a bare 400.
            throw new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Salesforce refused the login at " + config.loginUrl() + ": "
                            + truncate(response.body()));
        }

        JsonNode token = parse(response.body());
        this.accessToken = token.path("access_token").asText(null);
        this.instanceUrl = token.path("instance_url").asText(config.loginUrl());

        if (accessToken == null) {
            throw new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Salesforce returned no access token: " + truncate(response.body()));
        }
    }

    /** The Bulk API 2.0 base, e.g. {@code https://x.my.salesforce.com/services/data/v62.0/jobs}. */
    String jobsUrl() {
        return instanceUrl + "/services/data/" + config.apiVersion() + "/jobs";
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

    JsonNode patchJson(String url, String json, String what) {
        return parse(expectOk(request(url)
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8)),
                what).body());
    }

    /** Uploads a staged CSV file as the job's data. */
    void putCsv(String url, Path file, String what) {
        try {
            expectOk(request(url)
                    .header("Content-Type", "text/csv")
                    .PUT(HttpRequest.BodyPublishers.ofFile(file)), what);
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not read the staged upload file " + file + ": " + e.getMessage(), e);
        }
    }

    /** Fetches a CSV body, such as a job's failed or successful results. */
    String getCsv(String url, String what) {
        return expectOk(request(url).header("Accept", "text/csv").GET(), what).body();
    }

    /**
     * Fetches a page of query results together with the locator for the next one.
     *
     * <p>The locator is the only thing Salesforce will accept as a resume position, and it arrives
     * in a response header rather than in the body — so the body alone cannot tell a caller where
     * it got to. It answers the literal string {@code "null"} on the final page, which is not the
     * same as an absent header and must not be sent back as a locator.
     *
     * @param locator the position to resume from, or null for the first page
     */
    QueryPage getQueryPage(String url, String locator, String what) {
        String paged = locator == null || locator.isBlank() || "null".equals(locator)
                ? url
                : url + "&locator=" + URLEncoder.encode(locator, StandardCharsets.UTF_8);

        HttpResponse<String> response = expectOk(
                request(paged).header("Accept", "text/csv").GET(), what);

        String next = response.headers().firstValue("Sforce-Locator").orElse(null);
        return new QueryPage(response.body(), "null".equals(next) ? null : next);
    }

    /** A page of query results and where to continue from; {@code nextLocator} null means the end. */
    record QueryPage(String csv, String nextLocator) {
    }

    void delete(String url, String what) {
        expectOk(request(url).DELETE(), what);
    }

    private HttpRequest.Builder request(String url) {
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("Authorization", "Bearer " + accessToken);
    }

    /**
     * Sends, retrying once after re-authenticating on a 401.
     *
     * <p>One retry, not a loop. If a fresh token is also rejected the problem is the credential or
     * the org's policy, and hammering it is how an integration user gets locked out.
     */
    private HttpResponse<String> expectOk(HttpRequest.Builder builder, String what) {
        HttpResponse<String> response = send(builder, what);

        if (response.statusCode() == 401) {
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
     * <p>The distinction that matters is retryable versus not. A 503 or a request limit is worth
     * waiting out; a malformed SOQL query or a field that does not exist on the object will fail
     * identically for ever, and retrying it five times only delays the message that would have
     * told somebody what to fix.
     */
    private ConnectorException translate(HttpResponse<String> response, String what) {
        String detail = truncate(response.body());
        int status = response.statusCode();

        if (status == 401 || status == 403) {
            return new ConnectorException(ConnectorException.Kind.AUTHENTICATION,
                    "Salesforce refused to " + what + ": " + detail);
        }
        if (status == 400 || status == 404 || status == 422) {
            return new ConnectorException(ConnectorException.Kind.CONFIGURATION,
                    "Salesforce rejected the request to " + what + ": " + detail);
        }
        // 429 and 503 carry Retry-After when the org is throttling; honoured by the engine's own
        // backoff rather than slept on here, so a worker is not held hostage by a busy org.
        return new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                "Salesforce could not " + what + " (HTTP " + status + "): " + detail);
    }

    private HttpResponse<String> send(HttpRequest.Builder builder, String what) {
        try {
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Could not reach Salesforce to " + what + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Interrupted while waiting for Salesforce to " + what, e);
        }
    }

    private static JsonNode parse(String body) {
        try {
            return Json.mapper().readTree(body);
        } catch (IOException e) {
            throw new ConnectorException(ConnectorException.Kind.UNAVAILABLE,
                    "Salesforce returned a response that is not JSON: " + truncate(body), e);
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
