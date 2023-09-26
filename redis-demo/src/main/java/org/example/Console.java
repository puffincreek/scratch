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
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@SpringBootApplication
public class Console implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication.run(Console.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String host = readRedisHostFromStdIn(args);
        if (!StringUtils.hasLength(host)) {
            log.error("no host supplied. Default to localhost");
            host = "localhost";
        }

        testTlsConnection(host);
    }

    private void testTlsConnection(String host) {
        final GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10);

        final Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort(host, 6379));

        log.info("testing redis ops");
        try (final JedisCluster jedisCluster = new JedisCluster(nodes, 1000, 100, 1, null, "my-client", config, true)) {
            jedisCluster.set("some-key", "some-value");
            log.info("result: {}", jedisCluster.get("some-key"));
        } catch (final Exception e) {
            log.error("Root cause",  e.getCause());
        }
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