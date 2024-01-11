package io.confluent.bootcamp.ksql;

import io.confluent.ksql.api.client.ClientOptions;
import picocli.CommandLine;
import picocli.CommandLine.Option;

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

    public KSQLClient() {}

    @Override
    public Integer call() throws Exception {
        ClientOptions options = ClientOptions.create()
                .setBasicAuthCredentials("<ksqlDB-API-key>", "<ksqlDB-API-secret>")
                .setHost("<ksqlDB-endpoint>")
                .setPort(443)
                .setUseTls(true)
                .setUseAlpn(true);

        System.out.println("Hello world!");

        return null;
    }

    public static void main(String[] args) {
        new CommandLine(new KSQLClient()).execute(args);
    }

}