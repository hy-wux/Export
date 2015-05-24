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
package com.petasoft.export.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author ŒÈœ  2015-05-15
 */
public final class FileUtil {

	private static String fileEncode = System.getProperty("file.encoding");

	public static void overrid(String fileName, String content)
			throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		appendln(fileName, content);
	}

	public static void appendln(String fileName, String content)
			throws IOException {
		append(fileName, content);
		append(fileName, "\n");
	}

	public static void append(String fileName, String content)
			throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter writer = new FileWriter(file, true);
		writer.write(new String(content.getBytes(), fileEncode));
		writer.flush();
		writer.close();
	}
}
