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
package de.tu_dortmund.ub.data.dswarm;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.json.JsonObject;

import de.tu_dortmund.ub.data.TPUException;
import de.tu_dortmund.ub.data.util.TPUUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tgaengler
 */
public class TPUTask implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(TPUTask.class);

	private final Properties config;
	private final String watchFolderFile;
	private final String resourceWatchFolder;
	private final Optional<String> optionalOutputDataModelID;
	private final Optional<String> optionalExportMimeType;
	private final Optional<String> optionalExportFileExtension;
	private final String serviceName;
	private final int cnt;

	public TPUTask(final Properties config,
	               final String watchFolderFile,
	               final String resourceWatchFolder,
	               final Optional<String> optionalOutputDataModelID,
	               final Optional<String> optionalExportMimeType,
	               final Optional<String> optionalExportFileExtension,
	               final String serviceName,
	               final int cnt) {

		this.config = config;
		this.watchFolderFile = watchFolderFile;
		this.resourceWatchFolder = resourceWatchFolder;
		this.optionalOutputDataModelID = optionalOutputDataModelID;
		this.optionalExportMimeType = optionalExportMimeType;
		this.optionalExportFileExtension = optionalExportFileExtension;
		this.serviceName = serviceName;
		this.cnt = cnt;
	}

	@Override
	public String call() throws Exception {

		try {

			final Integer engineThreads = 1;
			final JsonObject initResultJSON = TPUUtil.doInit(resourceWatchFolder, watchFolderFile, serviceName, engineThreads, config, cnt);

			final String inputDataModelID = initResultJSON.getString(Init.DATA_MODEL_ID);

			final String outputDataModelID;

			if (optionalOutputDataModelID.isPresent()) {

				outputDataModelID = optionalOutputDataModelID.get();
			} else {

				LOG.info(
						"[{}[{}] couldn't find output data model ID, will take input data model id instead for processing the task on source file '{}' and data model '{}' (note: this might cause wrong behaviour!)",
						serviceName, cnt, watchFolderFile, inputDataModelID);

				outputDataModelID = inputDataModelID;
			}

			final String result = executeTransformation(inputDataModelID, outputDataModelID, optionalExportMimeType, optionalExportFileExtension, engineThreads, config, serviceName, cnt);

			final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

			TPUUtil.cleanUpMetadataRepository(initResultJSON, serviceName, engineDswarmAPI, cnt);

			return String
					.format("[%s][%d] TPU task execution '%d' result = '%s' for source file '%s' and data model '%s'", serviceName, cnt, cnt, result,
							watchFolderFile, inputDataModelID);
		} catch (final Exception e) {

			final String message = String
					.format("[%s][%d] TPU task execution '%d' failed for source file '%s'", serviceName, cnt, cnt, watchFolderFile);

			throw new TPUException(message, e);
		}
	}

	private static String executeTransformation(final String inputDataModelID,
	                                            final String outputDataModelID,
	                                            final Optional<String> optionalExportMimeType,
	                                            final Optional<String> optionalExportFileExtension,
	                                            final Integer engineThreads,
	                                            final Properties config,
	                                            final String serviceName, final int cnt) throws Exception {

		// create job
		final Optional<Boolean> optionalDoExportOnTheFly = Optional.of(Boolean.TRUE);
		final Optional<Boolean> optionalDoIngestOnTheFly = Optional.of(Boolean.TRUE);
		final Callable<String> transformTask = new Transform(config, inputDataModelID, outputDataModelID, optionalDoIngestOnTheFly,
				optionalDoExportOnTheFly, optionalExportMimeType, optionalExportFileExtension, cnt);

		// work on jobs
		final ThreadPoolExecutor pool = new ThreadPoolExecutor(engineThreads, engineThreads, 0L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<>());

		try {

			final List<Callable<String>> tasks = new LinkedList<>();
			tasks.add(transformTask);

			final List<Future<String>> futureList = pool.invokeAll(tasks);
			final Iterator<Future<String>> iterator = futureList.iterator();

			if (!iterator.hasNext()) {

				return "no result";
			}

			final Future<String> f = iterator.next();

			final String message = f.get();

			final String message1 = String.format("[%s][%d] %s", serviceName, cnt, message);

			LOG.info(message1);

			return message;
		} catch (final Exception e) {

			throw e;
		} finally {

			pool.shutdown();
		}
	}
}
