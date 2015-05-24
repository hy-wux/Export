/*
 * Copyright 2015 Petasoft Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.petasoft.export.conf;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 * 参数选项设置
 * 
 * @author 伍鲜 2015-05-15
 */
public class GenericOptionsParser {
	private CommandLine commandLine;

	public GenericOptionsParser(Tool tool, String[] args) throws Exception {
		parseGeneralOptions(tool, new Options(), args);
	}

	private String[] parseGeneralOptions(Tool tool, Options opts, String[] args)
			throws Exception {
		opts = buildGeneralOptions(opts);
		CommandLineParser parser = new PosixParser();
		try {
			commandLine = parser.parse(opts, args);
			processGeneralOptions(tool, commandLine);
			return commandLine.getArgs();
		} catch (ParseException e) {
			printGenericCommandUsage(opts);
		}
		return args;
	}

	/**
	 * Specify properties of each generic option
	 */
	@SuppressWarnings("static-access")
	public static Options buildGeneralOptions(Options opts) {
		Option c = OptionBuilder.hasArg(true)
				.withArgName("core configuration file")
				.withDescription("Specify an core configuration file")
				.create("c");
		Option d = OptionBuilder.hasArg(true)
				.withArgName("db configuration file")
				.withDescription("Specify an db configuration file")
				.create("d");
		Option e = OptionBuilder.hasArg(true)
				.withArgName("export configuration file")
				.withDescription("Specify an export configuration file")
				.create("e");
		Option m = OptionBuilder.hasArg(true)
				.withArgName("merge configuration file")
				.withDescription("Specify an merge configuration file")
				.create("m");
		Option f = OptionBuilder.hasArg(true)
				.withArgName("ftp configuration file")
				.withDescription("Specify an ftp configuration file")
				.create("f");
		Option help = OptionBuilder.hasArg(false)
				.withDescription("Get helpfull infomation about program")
				.create("help");

		opts.addOption(c);
		opts.addOption(d);
		opts.addOption(e);
		opts.addOption(m);
		opts.addOption(f);
		opts.addOption(help);
		return opts;
	}

	private void processGeneralOptions(Tool tool, CommandLine line)
			throws Exception {
		if (line.hasOption("c")) {
			tool.getConf().core_site = line.getOptionValue("c");
		}
		if (line.hasOption("d")) {
			tool.getConf().db_site = line.getOptionValue("d");
		}
		if (line.hasOption("e")) {
			tool.getConf().export_site = line.getOptionValue("e");
		}
		if (line.hasOption("m")) {
			tool.getConf().merge_site = line.getOptionValue("m");
		}
		if (line.hasOption("f")) {
			tool.getConf().ftp_site = line.getOptionValue("f");
		}
	}

	public String[] getRemainingArgs() {
		return (commandLine == null) ? new String[] {} : commandLine.getArgs();
	}

	public CommandLine getCommandLine() {
		return commandLine;
	}

	public static void printGenericCommandUsage() {
		printGenericCommandUsage(buildGeneralOptions(new Options()));
	}

	private static void printGenericCommandUsage(Options opts) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("Generic options supported are", opts);
		System.out.println();
		System.out.println("The general command line syntax is");
		System.out
				.println("Export [GenericOptions] TranDate [BeginTranTime] [EndTranTime]\n");
	}
}
