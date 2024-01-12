# KSQLJavaClient

A little tool used with bootcamp-rails (https://github.com/sknop/bootcamp-rails) to upload KSQL statements.

## Problem to be solved

I created a set of KSQL statements in a single file. Existing tools only allow me to send off single statements, and ignore or balk at SET statements to set options.
I also had a lot of files in a single directory, hence this tool.

- Read a single file or, optionally, a whole directory full of files sorted by name
- Parse the files (roughly) to extract the single statements. We are only distinguishing two kinds of statements:
  - SET statements, which will define options. Typically something like

        SET 'auto.offset.reset' = 'earliest';

  - Any other statement, such as 

        CREATE STREAM ...

## Usage

    KSQLClient [-hV] -c=<configFile> (-d=<ksqlDirectory> | -f=<ksqlFilename>)
    
    Description:
    
    Read KSQL statements and apply them.
    
    Options:
    
      -c, --config=<configFile>
                      ksqlDB connection configuration file
      -d, --directory=<ksqlDirectory>
                      Directory containing ksqlDB execution files to be processed
                        in order
      -f, --filename=<ksqlFilename>
                      Filename containing ksqlDB execution statements
      -h, --help      Show this help message and exit.
      -V, --version   Print version information and exit.

## Copyright
Copyright (C) 2024 Sven Erik Knop, Confluent
