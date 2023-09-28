package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

@Slf4j
@SpringBootApplication
public class Console implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Console.class, args);
    }

    @Override
    public void run(final String... args) throws Exception {
        String host = readRedisHostFromStdIn(args);
        if (!StringUtils.hasLength(host)) {
            log.error("no host supplied. Default to localhost");
            host = "localhost";
        }

        testTlsPool(host);
//        testTlsConnection(host);
    }

    static class LoggingJedisPool extends JedisPool {
        public LoggingJedisPool(final HostAndPort hostAndPort) {
            super(new GenericObjectPoolConfig<>(),
                    hostAndPort.getHost(),
                    hostAndPort.getPort(),
                    2000,
                    null,
                    null,
                    true);
        }

        @Override
        public Jedis getResource() {
            try {
                return internalPool.borrowObject();
            } catch (final Exception e) {
                log.error("Jedis exception", e);
            }
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private void testTlsPool(final String configHost) {
        final HostAndPort configHostNPort = new HostAndPort(configHost, 6379);
        try (final JedisPool pool = new LoggingJedisPool(configHostNPort)) {
            final Jedis jedis = testConnection(configHostNPort, pool);
            final List<Object> slots = jedis.clusterSlots();

            for (final Object slotInfoObj : slots) {
                List<Object> slotInfo = (List<Object>) slotInfoObj;
                if (slotInfo.size() <= 2) {
                    continue;
                }

                final List<Object> hostParameters = (List<Object>) slotInfo.get(2);
                if (hostParameters.isEmpty()) {
                    continue;
                }

                final HostAndPort node = generateHostAndPort(hostParameters);
                try (final JedisPool nodePool = new LoggingJedisPool(node)) {
                    testConnection(node, nodePool);
                }
            }
        }
    }

    private HostAndPort generateHostAndPort(final List<Object> hostParameters) {
        String host = SafeEncoder.encode((byte[]) hostParameters.get(0));
        int port = ((Long) hostParameters.get(1)).intValue();
        return new HostAndPort(host, port);
    }

    private Jedis testConnection(final HostAndPort hostAndPort,
                                 final JedisPool pool) {
        final Jedis jedis = pool.getResource();
        if (jedis == null) {
            log.error("no connection established towards: {}", hostAndPort);
        } else {
            log.info("connection established towards: {}", hostAndPort);
        }
        return jedis;
    }

    private String readRedisHostFromStdIn(final String[] args) throws ParseException {
        final Option redisHost = new Option("host", true, "redis cluster configuration endpoint");
        final Options options = new Options();
        options.addOption(redisHost);

        final CommandLineParser parser = new DefaultParser();

        try {
            final CommandLine cmd = parser.parse(options, args);
            return cmd.getOptionValue(redisHost);
        } catch (final ParseException e) {
            log.error("where is host?", e);
        }

        return null;
    }

}