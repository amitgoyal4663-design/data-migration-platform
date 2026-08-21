package com.dmp.connector.runtime;

import com.dmp.common.error.DmpException;
import com.dmp.common.error.ErrorCode;
import com.dmp.connector.api.Connector;
import com.dmp.connector.api.ConnectorSpec;
import com.dmp.connector.api.Sink;
import com.dmp.connector.api.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Stream;

/**
 * Discovers connectors and hands them out by type.
 *
 * <p>Two sources of connectors, both through {@link ServiceLoader}:
 *
 * <ul>
 *   <li><b>Classpath</b> — connectors shipped with the platform.</li>
 *   <li><b>Plugin directory</b> — one subdirectory per connector, each loaded by its own
 *       {@link PluginClassLoader} so their dependencies cannot collide.</li>
 * </ul>
 *
 * <p>Discovery happens once at startup. Hot reload is deliberately unsupported: discarding a
 * classloader whose classes are still referenced by a running chunk leaks the whole loaded graph,
 * and a worker restart makes the machinery to do it safely unnecessary.
 *
 * <p>Registration is by declaration, not configuration. A connector jar names itself in
 * {@code META-INF/services/com.dmp.connector.api.Connector} and appears; nothing in the platform
 * lists known connectors, which is what makes "add a connector without changing existing code"
 * literally true.
 */
@Component
public class ConnectorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ConnectorRegistry.class);

    private final Map<String, Registration> byType = new LinkedHashMap<>();
    private final Path pluginDirectory;

    public ConnectorRegistry(@Value("${dmp.connectors.plugin-directory:plugins}") String pluginDirectory) {
        this.pluginDirectory = Path.of(pluginDirectory);
        loadFromClasspath();
        loadFromPluginDirectory();
        log.info("Connector registry ready with {} connector(s): {}", byType.size(), byType.keySet());
    }

    /** Every discovered connector's specification, for the console's connector picker. */
    public List<ConnectorSpec> specs() {
        return byType.values().stream()
                .map(registration -> registration.connector().spec())
                .sorted(Comparator.comparing(ConnectorSpec::displayName))
                .toList();
    }

    public Optional<ConnectorSpec> spec(String type) {
        return Optional.ofNullable(byType.get(type)).map(r -> r.connector().spec());
    }

    /**
     * Returns a connector as a source.
     *
     * @throws DmpException if the type is unknown or the connector cannot read. Both are user-facing
     *         errors — a pipeline referencing a connector that is not installed, or wiring a
     *         write-only connector into a source node — so both name what is wrong rather than
     *         failing with a cast error deep in the engine.
     */
    public Source source(String type) {
        Connector connector = require(type);
        if (!(connector instanceof Source source)) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "Connector '" + type + "' cannot be used as a source",
                    Map.of("connectorType", type,
                            "direction", connector.spec().direction().name()));
        }
        return source;
    }

    public Sink sink(String type) {
        Connector connector = require(type);
        if (!(connector instanceof Sink sink)) {
            throw new DmpException(ErrorCode.INVALID_REFERENCE,
                    "Connector '" + type + "' cannot be used as a sink",
                    Map.of("connectorType", type,
                            "direction", connector.spec().direction().name()));
        }
        return sink;
    }

    public Connector require(String type) {
        Registration registration = byType.get(type);
        if (registration == null) {
            throw new DmpException(ErrorCode.NOT_FOUND,
                    "No connector of type '" + type + "' is installed. "
                            + "Install its plugin and restart the worker.",
                    Map.of("connectorType", String.valueOf(type),
                            "installed", byType.keySet().toString()));
        }
        return registration.connector();
    }

    public boolean isInstalled(String type) {
        return byType.containsKey(type);
    }

    private void loadFromClasspath() {
        ServiceLoader.load(Connector.class).forEach(connector -> register(connector, "classpath"));
    }

    private void loadFromPluginDirectory() {
        if (!Files.isDirectory(pluginDirectory)) {
            log.debug("No plugin directory at {}; using classpath connectors only",
                    pluginDirectory.toAbsolutePath());
            return;
        }

        try (Stream<Path> entries = Files.list(pluginDirectory)) {
            entries.filter(Files::isDirectory).sorted().forEach(this::loadPlugin);
        } catch (IOException e) {
            // A broken plugin directory must not prevent the worker from starting with the
            // connectors it does have. A pipeline needing a missing one fails with a clear message.
            log.error("Could not read plugin directory {}", pluginDirectory.toAbsolutePath(), e);
        }
    }

    private void loadPlugin(Path directory) {
        String name = directory.getFileName().toString();
        try (Stream<Path> files = Files.list(directory)) {
            List<URL> jars = new ArrayList<>();
            for (Path file : files.filter(p -> p.toString().endsWith(".jar")).sorted().toList()) {
                jars.add(file.toUri().toURL());
            }
            if (jars.isEmpty()) {
                log.warn("Plugin directory {} contains no jars", directory);
                return;
            }

            PluginClassLoader loader = new PluginClassLoader(
                    name, jars.toArray(URL[]::new), getClass().getClassLoader());

            ServiceLoader.load(Connector.class, loader)
                    .forEach(connector -> register(connector, "plugin:" + name));

        } catch (Exception e) {
            log.error("Failed to load plugin '{}' from {}; the worker will start without it",
                    name, directory, e);
        }
    }

    private void register(Connector connector, String origin) {
        ConnectorSpec spec;
        try {
            spec = connector.spec();
        } catch (Exception e) {
            log.error("Connector {} from {} failed to describe itself and was skipped",
                    connector.getClass().getName(), origin, e);
            return;
        }

        Registration existing = byType.get(spec.type());
        if (existing != null) {
            // First registration wins, so a plugin cannot silently shadow a platform connector.
            // Logged loudly because a duplicate type is almost always a packaging mistake.
            log.error("Connector type '{}' is provided by both {} and {}; keeping {}",
                    spec.type(), existing.origin(), origin, existing.origin());
            return;
        }

        byType.put(spec.type(), new Registration(connector, origin));
        log.info("Registered connector '{}' v{} ({}) from {}",
                spec.type(), spec.version(), spec.direction(), origin);
    }

    private record Registration(Connector connector, String origin) {
    }
}
