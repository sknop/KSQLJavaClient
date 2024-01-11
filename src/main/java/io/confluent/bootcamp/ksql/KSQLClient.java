package io.confluent.bootcamp.ksql;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.StreamInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        versionProvider = Version.class,
        description = "Read KSQL statements and apply them."
)

public class KSQLClient implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(KSQLClient.class);

    @Option(names = {"-c", "--config"}, required = true, description = "ksqlDB connection configuration file")
    String configFile;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
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


    public KSQLClient() {}

    private void processConfigFile() {
        logger.info("Processing config file {}", configFile);

        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);

            ksqlAPIKey = properties.getProperty("api.key");
            ksqlAPISecret = properties.getProperty("api.secret");
            ksqlDbEndpoint = properties.getProperty("ksqldb.endpoint");

            logger.info("ksqlAPIKey = {}", ksqlAPIKey);
            logger.info("ksqlAPISecret = {}", ksqlAPISecret);
            logger.info("ksqlDbEndpoint = {}", ksqlDbEndpoint);

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
         }

         return null;
     }

    public static void main(String[] args) {
        new CommandLine(new KSQLClient()).execute(args);
    }

}