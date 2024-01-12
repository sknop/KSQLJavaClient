package io.confluent.bootcamp.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        versionProvider = Version.class,
        description = "Read KSQL statements and apply them."
)

public class KSQLClient implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(KSQLClient.class);

    @Option(names = {"-c", "--config"}, required = true, description = "ksqlDB connection configuration file")
    String configFile;

    @CommandLine.ArgGroup(multiplicity = "1")
    Exclusive exclusive;

    static class Exclusive {
        @Option(names = {"-d", "--directory"}, description = "Directory containing ksqlDB execution files to be processed in order")
        String ksqlDirectory;

        @Option(names = {"-f", "--filename"}, description = "Filename containing ksqlDB execution statements")
        String ksqlFilename;
    }

    private String ksqlAPIKey;
    private String ksqlAPISecret;
    private String ksqlDbEndpoint;


    public KSQLClient() {
    }

    static class StatementParser {
        private final Map<String, Object> properties = new HashMap<>();
        BufferedReader reader;
        String nextStatement = "";

        Pattern setPattern = Pattern.compile("SET '(.*)'\\s*=\\s*'(.*)'\\s*;");

        StatementParser(String fileContent) {
            this.reader = new BufferedReader(new StringReader(fileContent));
        }

        boolean processNextStatement() throws IOException {
            String nextLine = reader.readLine();
            if (nextLine == null) {
                return false;
            }

            String line = nextLine.trim();

            // Check for and ignore comments or empty lines
            if (line.startsWith("--") || line.isEmpty()) {
                return processNextStatement();
            }

            if (line.startsWith("SET")) {
                Matcher matcher = setPattern.matcher(line);
                if (matcher.find()) {
                    String key = matcher.group(1);
                    String value = matcher.group(2);

                    properties.put(key, value);
                }
                else {
                    logger.warn("SetPattern did not match '{}'", line);
                }
            }
            else {
                if (line.endsWith(";")) {
                    nextStatement = line;

                    return true;
                }
                StringBuilder builder = new StringBuilder(line);

                String l;
                do {
                    String nl = reader.readLine();
                    logger.info("Next line '{}'", nl);
                    if (nl == null) {
                        logger.error("Incomplete statement '{}'", builder);
                        return false;
                    }
                    l = nl.trim();

                    logger.info("Does l end with ;? {}", l.endsWith(";"));

                    builder.append("\n");
                    builder.append(l);
                } while (! l.endsWith(";"));

                nextStatement = builder.toString();
                return true;
            }

            logger.error("Should never get here.");
            return false;
        }

        boolean hasNextStatement() {
            try {
                return processNextStatement();
            } catch (IOException e) {
                logger.error("Should never happen!", e);
                throw new RuntimeException(e);
            }
        }

        String nextStatement() {
            return nextStatement;
        }
    }
    private void processOneKSQLStatement(Client client, String statement, Map<String, Object> properties) {
        try {
            ExecuteStatementResult results = client.executeStatement(statement, properties).get();

            System.out.println(results);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Encountered exception while executing: \n\n{}\n", statement, e);
            throw new RuntimeException(e);
        }
    }

    private void processOneKSQLFile(Client client, String filename) {
        logger.info("Processing {}", filename);

        try {
            String fileContent = new String(Files.readAllBytes(Paths.get(filename)));

            // parse file and process on statement at a time
            StatementParser statementParser = new StatementParser(fileContent);

            while(statementParser.hasNextStatement()) {
                String statement = statementParser.nextStatement();

                processOneKSQLStatement(client, statement, statementParser.properties);
            }
        } catch (IOException e) {
            logger.error("I/O Error while processing {}", filename, e);
            throw new RuntimeException(e);
        }

    }

    private void processKSQLDirectory(Client client, String directoryName) {
        List<String> files = Stream.of(Objects.requireNonNull(new File(directoryName).listFiles())).
                filter(file -> !file.isDirectory()).
                map(File::getName).
                sorted().
                collect(Collectors.toList());

        for (var file : files) {
            processOneKSQLFile(client, file);
        }
    }

    private void processConfigFile() {
        logger.info("Processing config file {}", configFile);

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);

            ksqlAPIKey = properties.getProperty("api.key");
            ksqlAPISecret = properties.getProperty("api.secret");
            ksqlDbEndpoint = properties.getProperty("ksqldb.endpoint");

            logger.debug("ksqlAPIKey = {}", ksqlAPIKey);
            logger.debug("ksqlAPISecret = {}", ksqlAPISecret);
            logger.debug("ksqlDbEndpoint = {}", ksqlDbEndpoint);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer call() throws Exception {
        processConfigFile();

        ClientOptions options = ClientOptions.create()
                .setBasicAuthCredentials(ksqlAPIKey, ksqlAPISecret)
                .setHost(ksqlDbEndpoint)
                .setPort(443)
                .setUseTls(true)
                .setUseAlpn(true);

        try (Client client = Client.create(options)) {
            List<StreamInfo> streams = client.listStreams().get();
            for (StreamInfo stream : streams) {
                System.out.println(
                        stream.getName()
                                + " " + stream.getTopic()
                                + " " + stream.getKeyFormat()
                                + " " + stream.getValueFormat()
                                + " " + stream.isWindowed()
                );
            }
            if (exclusive.ksqlDirectory != null) {
                processKSQLDirectory(client, exclusive.ksqlDirectory);
            }
            else if (exclusive.ksqlFilename != null) {
                processOneKSQLFile(client, exclusive.ksqlFilename);
            }
        }

        return null;
    }

    public static void main(String[] args) {
        new CommandLine(new KSQLClient()).execute(args);
    }

}