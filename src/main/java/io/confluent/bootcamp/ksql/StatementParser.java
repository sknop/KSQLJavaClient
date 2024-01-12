package io.confluent.bootcamp.ksql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class StatementParser {
    private final static Logger logger = LoggerFactory.getLogger(StatementParser.class);
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
                return processNextStatement();
            } else {
                logger.warn("SetPattern did not match '{}'", line);
                return false;
            }
        } else {
            if (line.endsWith(";")) {
                nextStatement = line;

                return true;
            }
            StringBuilder builder = new StringBuilder(line);

            do {
                nextLine = reader.readLine();
                if (nextLine == null) {
                    logger.error("Incomplete statement '{}'", builder);
                    return false;
                }
                line = nextLine.trim();

                builder.append("\n");
                builder.append(line);
            } while (!line.endsWith(";"));

            nextStatement = builder.toString();
            return true;
        }
    }

    boolean hasNextStatement() {
        try {
            boolean hasNextMessage = processNextStatement();

            if (! hasNextMessage) {
                reader.close();
                nextStatement = "";
            }

            return hasNextMessage;
        } catch (IOException e) {
            logger.error("Should never happen!", e);
            throw new RuntimeException(e);
        }
    }

    String nextStatement() {
        return nextStatement;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
