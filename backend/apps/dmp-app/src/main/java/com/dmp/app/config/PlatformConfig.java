package com.dmp.app.config;

import com.dmp.common.json.Json;
import com.dmp.domain.pipeline.PipelineValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Clock;

/** Platform-wide beans. */
@Configuration
public class PlatformConfig {

    /**
     * The single {@link ObjectMapper} used everywhere.
     *
     * <p>Replaces Spring's auto-configured mapper deliberately. A record serialised by the web
     * layer and the same record serialised by the engine must produce identical JSON — otherwise a
     * decimal that survives an API round trip is silently truncated on its way through a pipeline.
     * Per ADR-0003 {@code JsonNode} is the payload model, which makes this configuration a
     * correctness concern rather than a formatting preference.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return Json.mapper();
    }

    /**
     * Applies the platform's settings to any mapper Spring Boot builds for its own purposes.
     *
     * <p>Belt and braces: some auto-configurations construct their own mapper from the builder
     * rather than injecting the primary bean.
     */
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer platformJacksonCustomizer() {
        return builder -> builder.configure(Json.mapper());
    }

    /**
     * A single {@link Clock}, injected rather than read statically.
     *
     * <p>Every timestamp in the domain arrives as a parameter, so tests fix time instead of
     * tolerating it — which is what makes assertions about retry backoff, TTL expiry and run
     * duration deterministic rather than flaky.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /** Stateless, so one instance is shared. */
    @Bean
    public PipelineValidator pipelineValidator() {
        return new PipelineValidator();
    }
}
