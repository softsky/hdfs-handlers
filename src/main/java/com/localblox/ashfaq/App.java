package com.localblox.ashfaq;

import static org.apache.commons.cli.OptionBuilder.withLongOpt;

import com.localblox.ashfaq.config.AppConfig;
import com.localblox.ashfaq.filewatcher.HdfsFileWatcher;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Options options = new Options();

        options.addOption(withLongOpt("help").withDescription("show help.").create("h"));
        options.addOption(withLongOpt("hdfsAdminUri").hasArg().withDescription("HDFS admin URI").isRequired().create("hdfsUri"));
        options.addOption(withLongOpt("s3accessKey").hasArg().withDescription("access key id for s3.").create("s3a"));
        options.addOption(withLongOpt("s3secretAccessKey").hasArg().withDescription("secret access key for s3.").create("s3s"));
//        options.addOption(OptionBuilder.withLongOpt("s3passwd").hasArg().withDescription("password for for s3.").isRequired().create("s3p"));

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h"))
                help(options);
        } catch (ParseException e) {
            log.error("Failed to parse comand line properties", e);
            help(options);
        }

        AppConfig.initConfig(cmd.getOptionValue("s3a"), cmd.getOptionValue("s3s"), cmd.getOptionValue("s3p"));

        //approach #1: use own File watcher
        HdfsFileWatcher watcher = new HdfsFileWatcher(cmd.getOptionValue("hdfsUri"));

        watcher.start();

    }

    private static void help(Options options) {
        // This prints out some help
        HelpFormatter formatter = new HelpFormatter();

        formatter.printHelp("Main", options);
        System.exit(0);
    }
}
