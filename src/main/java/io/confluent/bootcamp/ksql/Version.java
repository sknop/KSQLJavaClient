package io.confluent.bootcamp.ksql;

import picocli.CommandLine;

public class Version implements CommandLine.IVersionProvider {
    public String[] getVersion() {
        Package mainPackage = KSQLClient.class.getPackage();
        String version = mainPackage.getImplementationVersion();

        return new String[] { version };
    }
}
