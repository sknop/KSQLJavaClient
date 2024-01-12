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
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CommandLine.Command(
        name = "KSQLClient",
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

    @Option(names = {"-l", "--list-streams"}, description = "List existing streams before and after statements have been run.")
    boolean listStreams;

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

    private String formatStatement(String statement) {
        return statement.lines().map(line -> "\t" + line).collect(Collectors.joining("\n"));
    }

    private void processOneKSQLStatement(Client client, String statement, Map<String, Object> properties) {
        try {
            logger.debug("Processing statement\n{}", formatStatement(statement));
            logger.debug("Options: {}", properties);

            ExecuteStatementResult results = client.executeStatement(statement, properties).get();

            logger.debug(results.toString());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Encountered exception while executing: \n\n{}\n", formatStatement(statement), e);
            throw new RuntimeException(e);
        }
    }

    private void processOneKSQLFile(Client client, String filename) {
        logger.debug("Processing {}", filename);

        try {
            String fileContent = new String(Files.readAllBytes(Paths.get(filename)));

            // parse file and process on statement at a time
            StatementParser statementParser = new StatementParser(fileContent);

            while(statementParser.hasNextStatement()) {
                String statement = statementParser.nextStatement();

                processOneKSQLStatement(client, statement, statementParser.getProperties());
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
        logger.debug("Processing config file {}", configFile);

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
            if (listStreams)
                listStreams(client);

            if (exclusive.ksqlDirectory != null) {
                processKSQLDirectory(client, exclusive.ksqlDirectory);
            }
            else if (exclusive.ksqlFilename != null) {
                processOneKSQLFile(client, exclusive.ksqlFilename);
            }
            else {
                throw new RuntimeException("Should never get here.");
            }
            if (listStreams)
                listStreams(client);
        }

        return null;
    }

    private static void listStreams(Client client) throws InterruptedException, ExecutionException {
        List<StreamInfo> streams = client.listStreams().get();
        for (StreamInfo stream : streams) {
            logger.info("name = {} topic = {} keyFormat = {} valueFormat = {} isWindowed = {}",
                    stream.getName(),
                    stream.getTopic(),
                    stream.getKeyFormat(),
                    stream.getValueFormat(),
                    stream.isWindowed());
        }
    }

    public static void main(String[] args) {
        new CommandLine(new KSQLClient()).execute(args);
    }

}