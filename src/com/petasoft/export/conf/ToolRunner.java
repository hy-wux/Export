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

/**
 * @author ŒÈœ  2015-05-15
 */
public class ToolRunner {

	public static int run(Tool tool, String[] args) throws Exception {
		GenericOptionsParser parser = new GenericOptionsParser(tool, args);
		if (parser.getCommandLine() != null
				&& parser.getCommandLine().hasOption("help")) {
			printGenericCommandUsage();
			return 0;
		} else {
			args = parser.getRemainingArgs();
			return tool.run(args);
		}
	}

	public static void printGenericCommandUsage() {
		GenericOptionsParser.printGenericCommandUsage();
	}
}
