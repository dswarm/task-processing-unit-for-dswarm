/**
 * Copyright © 2015 – 2017 Dortmund University Library, SLUB Dresden & Avantgarde Labs GmbH (<code@dswarm.org>)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.dswarm;

import de.tu_dortmund.ub.data.TPUException;
import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.dswarm.common.DSWARMException;
import org.dswarm.common.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonObject;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker, M.L.I.S. (UB Dortmund)
 * @version 2015-04-20
 */
public final class TaskProcessingUnit {

	private static final Logger LOG = LoggerFactory.getLogger(TaskProcessingUnit.class);

	private static final String DEFAULT_CONFIG_PROPERTIES_FILE_NAME = "config.properties";
	private static final String DEFAULT_CONF_FOLDER_NAME = "conf";

	public static final String XML_FILE_ENDING = "xml";

	public static void main(final String[] args) throws Exception {

		// default config
		String configFile = DEFAULT_CONF_FOLDER_NAME + File.separatorChar + DEFAULT_CONFIG_PROPERTIES_FILE_NAME;

		// read program parameters
		if (args.length > 0) {

			for (final String arg : args) {

				LOG.info("arg = " + arg);

				if (arg.startsWith("-conf=")) {

					configFile = arg.split("=")[1];
				}
			}
		}

		final Properties config;

		// Init properties
		try {

			try (final InputStream inputStream = new FileInputStream(configFile)) {

				try (final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, TPUUtil.UTF_8))) {

					config = new Properties();
					config.load(reader);
				}
			}
		} catch (final IOException e) {

			LOG.error("something went wrong", e);
			LOG.error(String.format("FATAL ERROR: Could not read '%s'!", configFile));

			throw e;
		}

		startTPU(configFile, config);
	}

	public static String startTPU(final String confFile, final Properties config) throws Exception {

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		LOG.info(String.format("[%s] Starting 'Task Processing Unit' ...", serviceName));
		LOG.info(String.format("[%s] conf-file = %s", serviceName, confFile));

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		String[] watchFolderFiles = new File(resourceWatchFolder).list();

		if (watchFolderFiles == null) {

			final String message = String
					.format("could not determine files from watchfolder '%s'; watch folder file list does not exist", resourceWatchFolder);

			TaskProcessingUnit.LOG.error(message);

			throw new TPUException(message);
		}

		if (watchFolderFiles.length == 0) {

			final String message = String
					.format("could not determine files from watchfolder; there are no files in folder '%s'", resourceWatchFolder);

			TaskProcessingUnit.LOG.error(message);

			throw new TPUException(message);
		}

		Arrays.sort(watchFolderFiles);

		final String filesMessage = String
				.format("[%s] '%s' files in resource watch folder '%s'", serviceName, watchFolderFiles.length, resourceWatchFolder);

		LOG.info(filesMessage);
		LOG.info("\tfile names: '" + Arrays.toString(watchFolderFiles) + "'");

		// Init time counter
		final long global = System.currentTimeMillis();

		final Integer engineThreads = Integer.parseInt(config.getProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER));

		final Optional<Boolean> optionalDoInit = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INIT_IDENTIFIER, config);
		final Optional<Boolean> optionalDoTransformations = TPUUtil.getBooleanConfigValue(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER, config);
		final Optional<Boolean> optionalAllowMultipleDataModels = TPUUtil.getBooleanConfigValue(TPUStatics.ALLOW_MULTIPLE_DATA_MODELS_IDENTIFIER,
				config);
		final Optional<Boolean> optionalDoIngestOnTheFly = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INGEST_ON_THE_FLY_IDENTIFIER, config);
		final Optional<Boolean> optionalDoExportOnTheFly = TPUUtil.getBooleanConfigValue(TPUStatics.DO_EXPORT_ON_THE_FLY_IDENTIFIER, config);
		final Optional<String> optionalOutputDataModelID = TPUUtil.getStringConfigValue(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER, config);

		final Optional<String> optionalExportMimeType;

		if (optionalDoExportOnTheFly.isPresent() && optionalDoExportOnTheFly.get()) {

			optionalExportMimeType = TPUUtil.getStringConfigValue(TPUStatics.EXPORT_MIME_TYPE, config);
		} else {

			optionalExportMimeType = Optional.empty();
		}

		final Optional<String> optionalExportFileExtension;

		if (optionalExportMimeType.isPresent()) {

			String exportFileExtension;
			try {

				final MediaType mediaType = MediaType.getMediaTypeByName(optionalExportMimeType.get());
				exportFileExtension = mediaType.getFileExtension();
			} catch (final DSWARMException e) {

				// xml as default file extension
				exportFileExtension = XML_FILE_ENDING;
			}

			optionalExportFileExtension = Optional.ofNullable(exportFileExtension);
		} else {

			optionalExportFileExtension = Optional.empty();
		}

		final String result;

		if (goMultiThreaded(optionalDoInit, optionalDoTransformations, optionalAllowMultipleDataModels, optionalDoIngestOnTheFly,
				optionalDoExportOnTheFly)) {

			result = executeTPUTask(watchFolderFiles, resourceWatchFolder, optionalOutputDataModelID, optionalExportMimeType, optionalExportFileExtension, engineThreads, serviceName, config);
		} else {

			executeTPUPartsOnDemand(optionalDoInit, optionalAllowMultipleDataModels, watchFolderFiles, resourceWatchFolder, optionalOutputDataModelID,
					serviceName, engineThreads, optionalDoTransformations, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, optionalExportMimeType, optionalExportFileExtension, config);

			result = "[no result available]";
		}

		final String tasksExecutedMessage = String
				.format("[%s] d:swarm tasks executed. (Processing time: %d s)", serviceName, (
						(System.currentTimeMillis() - global) / 1000));

		LOG.info(tasksExecutedMessage);

		return result;
	}

	private static boolean goMultiThreaded(final Optional<Boolean> optionalDoInit, final Optional<Boolean> optionalDoTransformations,
	                                       final Optional<Boolean> optionalAllowMultipleDataModels, final Optional<Boolean> optionalDoIngestOnTheFly,
	                                       final Optional<Boolean> optionalDoExportOnTheFly) {

		return optionalDoInit.isPresent() && optionalDoInit.get() &&
				optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get() &&
				optionalDoTransformations.isPresent() && optionalDoTransformations.get() &&
				optionalDoIngestOnTheFly.isPresent() && optionalDoIngestOnTheFly.get() &&
				optionalDoExportOnTheFly.isPresent() && optionalDoExportOnTheFly.get();
	}

	private static String executeTPUTask(final String[] watchFolderFiles,
	                                     final String resourceWatchFolder,
	                                     final Optional<String> optionalOutputDataModelID,
	                                     final Optional<String> optionalExportMimeType,
	                                     final Optional<String> optionalExportFileExtension,
	                                     final Integer engineThreads,
	                                     final String serviceName,
	                                     final Properties config) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();

		int cnt = 1;

		for (final String watchFolderFile : watchFolderFiles) {

			LOG.info("[{}][{}] do TPU task execution '{}' for file '{}'", serviceName, cnt, cnt, watchFolderFile);

			transforms.add(new TPUTask(config, watchFolderFile, resourceWatchFolder, optionalOutputDataModelID, optionalExportMimeType, optionalExportFileExtension, serviceName, cnt));

			cnt++;
		}

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Future<String>> futureList = pool.invokeAll(transforms);

			final StringBuilder resultSB = new StringBuilder();

			for (final Future<String> f : futureList) {

				final String message = f.get();

				LOG.info(message);

				resultSB.append(message).append("\n");
			}

			return resultSB.toString();
		} catch (final Exception e) {

			LOG.error("something went wrong", e);

			throw e;
		} finally {

			pool.shutdown();
		}
	}

	private static void executeTPUPartsOnDemand(final Optional<Boolean> optionalDoInit,
	                                            final Optional<Boolean> optionalAllowMultipleDataModels,
	                                            String[] watchFolderFiles,
	                                            final String resourceWatchFolder,
	                                            final Optional<String> optionalOutputDataModelID,
	                                            final String serviceName,
	                                            final Integer engineThreads,
	                                            final Optional<Boolean> optionalDoTransformations,
	                                            final Optional<Boolean> optionalDoIngestOnTheFly,
	                                            final Optional<Boolean> optionalDoExportOnTheFly,
	                                            final Optional<String> optionalExportMimeType,
	                                            final Optional<String> optionalExportFileExtension,
	                                            final Properties config) throws Exception {

		// keys = input data models; values = related data resources
		final Map<String, Triple<String, String, String>> inputDataModelsAndResources = new HashMap<>();

		// init
		if (optionalDoInit.isPresent() && optionalDoInit.get()) {

			if (optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get()) {

				for (int i = 0; i < watchFolderFiles.length; i++) {

					final String initResourceFileName = watchFolderFiles[i];

					doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, inputDataModelsAndResources);

					// remove the file already processed during init from the files list to avoid duplicates
					watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
				}
			} else {

				// use the first file in the folder for init
				final String initResourceFileName = watchFolderFiles[0];

				doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, inputDataModelsAndResources);

				// remove the file already processed during init from the files list to avoid duplicates
				watchFolderFiles = ArrayUtils.removeElement(watchFolderFiles, initResourceFileName);
			}
		} else {

			final String inputDataModelID = config.getProperty(TPUStatics.PROTOTYPE_INPUT_DATA_MODEL_ID_IDENTIFIER);
			final String resourceID = config.getProperty(TPUStatics.PROTOTYPE_RESOURCE_ID_INDENTIFIER);

			inputDataModelsAndResources.put(inputDataModelID, Triple.of(inputDataModelID, resourceID, null));

			LOG.info("skip init part");
		}

		final Optional<Boolean> optionalDoIngest = TPUUtil.getBooleanConfigValue(TPUStatics.DO_INGEST_IDENTIFIER, config);

		// ingest
		if (optionalDoIngest.isPresent() && optionalDoIngest.get()) {

			final String projectName = config.getProperty(TPUStatics.PROJECT_NAME_IDENTIFIER);

			if (!optionalAllowMultipleDataModels.isPresent() || !optionalAllowMultipleDataModels.get()) {

				final Set<Map.Entry<String, Triple<String, String, String>>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, Triple<String, String, String>>> iterator = entries.iterator();
				final Map.Entry<String, Triple<String, String, String>> entry = iterator.next();

				final String inputDataModelID = entry.getKey();
				final Triple<String, String, String> triple = entry.getValue();
				final String resourceID = triple.getMiddle();

				executeIngests(watchFolderFiles, inputDataModelID, resourceID, projectName, serviceName, engineThreads, config);
			}
		} else {

			LOG.info("skip ingest");
		}

		if (!optionalOutputDataModelID.isPresent()) {

			throw new Exception("please set an output data model ('prototype.outputDataModelID') for this TPU task");
		}

		final String outputDataModelID = optionalOutputDataModelID.get();

		// task execution
		if (optionalDoTransformations.isPresent() && optionalDoTransformations.get()) {

			if (optionalAllowMultipleDataModels.isPresent() && optionalAllowMultipleDataModels.get()) {

				final Set<Map.Entry<String, Triple<String, String, String>>> entries = inputDataModelsAndResources.entrySet();

				for (final Map.Entry<String, Triple<String, String, String>> entry : entries) {

					final String inputDataModelID = entry.getKey();

					executeTransform(inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, optionalExportMimeType, optionalExportFileExtension, engineThreads,
							serviceName, config);
				}
			} else {

				final Set<Map.Entry<String, Triple<String, String, String>>> entries = inputDataModelsAndResources.entrySet();
				final Iterator<Map.Entry<String, Triple<String, String, String>>> iterator = entries.iterator();
				final Map.Entry<String, Triple<String, String, String>> entry = iterator.next();

				final String inputDataModelID = entry.getKey();

				executeTransform(inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, optionalExportMimeType, optionalExportFileExtension, engineThreads, serviceName,
						config);
			}
		} else {

			LOG.info("skip transformations");
		}

		final Optional<Boolean> optionalDoExport = TPUUtil.getBooleanConfigValue(TPUStatics.DO_EXPORT_IDENTIFIER, config);

		// export
		if (optionalDoExport.isPresent() && optionalDoExport.get()) {

			if (!optionalAllowMultipleDataModels.isPresent() || !optionalAllowMultipleDataModels.get()) {

				final String exportDataModelID;

				if (outputDataModelID != null && !outputDataModelID.trim().isEmpty()) {

					exportDataModelID = outputDataModelID;
				} else {

					final Set<Map.Entry<String, Triple<String, String, String>>> entries = inputDataModelsAndResources.entrySet();
					final Iterator<Map.Entry<String, Triple<String, String, String>>> iterator = entries.iterator();
					final Map.Entry<String, Triple<String, String, String>> entry = iterator.next();

					exportDataModelID = entry.getKey();
				}

				executeExport(exportDataModelID, optionalExportMimeType, optionalExportFileExtension, engineThreads, serviceName, config);
			}
		} else {

			LOG.info("skip export");
		}

		// clean-up
		int cnt = 0;

		final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

		final Set<Map.Entry<String, Triple<String, String, String>>> entries = inputDataModelsAndResources.entrySet();

		for (final Map.Entry<String, Triple<String, String, String>> entry : entries) {

			final Triple<String, String, String> triple = entry.getValue();

			final String inputDataModelId = triple.getLeft();
			final String resourceId = triple.getMiddle();
			final String configurationId = triple.getRight();

			TPUUtil.deleteObject(inputDataModelId, DswarmBackendStatics.DATAMODELS_ENDPOINT, serviceName, engineDswarmAPI, cnt);
			TPUUtil.deleteObject(resourceId, DswarmBackendStatics.RESOURCES_ENDPOINT, serviceName, engineDswarmAPI, cnt);
			TPUUtil.deleteObject(configurationId, DswarmBackendStatics.CONFIGURATIONS_ENDPOINT, serviceName, engineDswarmAPI, cnt);

			cnt++;
		}
	}

	private static void executeIngests(final String[] files, final String dataModelID, final String resourceID, final String projectName,
	                                   final String serviceName, final Integer engineThreads, final Properties config) throws Exception {

		// create job list
		final LinkedList<Callable<String>> filesToPush = new LinkedList<>();

		int cnt = 0;
		for (final String file : files) {

			cnt++;
			filesToPush.add(new Ingest(config, file, dataModelID, resourceID, projectName, cnt));
		}

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		execute(serviceName, filesToPush, pool);
	}


	private static void executeTransform(final String inputDataModelID,
	                                     final String outputDataModelID,
	                                     final Optional<Boolean> optionalDoIngestOnTheFly,
	                                     final Optional<Boolean> optionalDoExportOnTheFly,
	                                     final Optional<String> optionalExportMimeType,
	                                     final Optional<String> optionalExportFileExtension,
	                                     final Integer engineThreads,
	                                     final String serviceName,
	                                     final Properties config) throws Exception {

		// create job list
		final LinkedList<Callable<String>> transforms = new LinkedList<>();
		transforms.add(new Transform(config, inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly, optionalDoExportOnTheFly, optionalExportMimeType, optionalExportFileExtension, 0));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

		execute(serviceName, transforms, pool);
	}

	private static void executeExport(final String exportDataModelID,
	                                  final Optional<String> optionalExportMimeType,
	                                  final Optional<String> optionalExportFileExtension,
	                                  final Integer engineThreads,
	                                  final String serviceName,
	                                  final Properties config) throws Exception {

		// create job list
		final LinkedList<Callable<String>> exports = new LinkedList<>();
		exports.add(new Export(exportDataModelID, optionalExportMimeType, optionalExportFileExtension, config));

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		execute(serviceName, exports, pool);
	}

	private static void doInit(final String resourceWatchFolder, final String initResourceFileName, final String serviceName,
	                           final Integer engineThreads, final Properties config, final Map<String, Triple<String, String, String>> inputDataModelsAndResources)
			throws Exception {

		final JsonObject initResultJSON = TPUUtil.doInit(resourceWatchFolder, initResourceFileName, serviceName, engineThreads, config, 0);

		final String inputDataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);
		final String resourceID = initResultJSON.getString(Init.RESOURCE_ID);
		final String configurationID = initResultJSON.getString(Init.CONFIGURATION_ID);

		inputDataModelsAndResources.put(inputDataModelID, Triple.of(inputDataModelID, resourceID, configurationID));
	}

	private static void execute(final String serviceName,
	                            final LinkedList<Callable<String>> threads,
	                            final ThreadPoolExecutor pool) throws InterruptedException, java.util.concurrent.ExecutionException {

		try {

			final List<Future<String>> futureList = pool.invokeAll(threads);

			for (final Future<String> f : futureList) {

				final String message = f.get();

				final String message1 = String.format("[%s] %s", serviceName, message);

				LOG.info(message1);
			}

		} catch (final Exception e) {

			LOG.error("something went wrong", e);

			throw e;

		} finally {

			pool.shutdown();
		}
	}
}
