package com.dmp.app.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OpenAPI document metadata.
 *
 * <p>The generated specification is not only documentation. From Phase 8 it is the source for the
 * console's typed TypeScript client, which is the reason backend and frontend live in one
 * repository — split across two, every API change would open a version-skew window between the
 * spec and its consumer.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI dmpOpenApi() {
        return new OpenAPI().info(new Info()
                .title("Enterprise Data Migration Platform API")
                .version("v1")
                .description("""
                        Control-plane API for pipelines, versions, connector instances and runs.

                        **Core model.** A *pipeline* is a named container. A *pipeline version* is
                        an immutable snapshot of its DAG. A *run* executes one specific version.
                        Versions are frozen on publication so that a run started weeks ago remains
                        a truthful record of what executed, however much the pipeline has been
                        edited since.

                        **Errors** are RFC 7807 problem details. Every response carries a stable
                        `code` and a `retryable` flag, so clients can decide whether to retry
                        without parsing prose.

                        **Tenancy.** Every request is tenant-scoped via the `X-Tenant-Id` header.
                        Authentication is deferred pending SSO; the header is a development
                        posture and is not an authorisation mechanism.
                        """)
                .contact(new Contact().name("Platform Engineering"))
                .license(new License().name("Proprietary")));
    }
}
