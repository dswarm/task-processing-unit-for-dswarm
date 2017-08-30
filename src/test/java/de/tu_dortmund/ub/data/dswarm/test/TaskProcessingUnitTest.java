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
package de.tu_dortmund.ub.data.dswarm.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;

import com.google.common.io.Resources;
import de.tu_dortmund.ub.data.dswarm.TPUStatics;
import de.tu_dortmund.ub.data.dswarm.TaskProcessingUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * note: to execute the tests you first need to provide a D:SWARM backend and configure the API endpoint vie tpu.properties (parameter = 'dmp_api_endpoint')
 * + deploy the D:SWARM metadata repository dump at 'src/test/resources/metadata.sql' the D:SWARM instance that should be utilised for testing
 * + you probably need to execute the tests via 'mvn clean test'.
 *
 * @author tgaengler
 */
public class TaskProcessingUnitTest {

	private static final Logger LOG = LoggerFactory.getLogger(TaskProcessingUnitTest.class);

	private static final String DEFAULT_PROTOTYPE_PROJECT_ID           = "6db219f9-bfd1-76ff-36c6-d49d8057fd02";
	private static final String DEFAULT_DO_INIT                        = "true";
	private static final String DEFAULT_DO_INITIAL_DATA_MODEL_INGEST   = "false";
	private static final String DEFAULT_ALLOW_MULTIPLE_DATA_MODELS     = "true";
	private static final String DEFAULT_DO_INGEST                      = "false";
	private static final String DEFAULT_DO_TRANSFORMATIONS             = "true";
	private static final String DEFAULT_DO_INGEST_ON_THE_FLY           = "true";
	private static final String DEFAULT_DO_EXPORT_ON_THE_FLY           = "true";
	private static final String DEFAULT_DO_EXPORT                      = "false";
	private static final String DEFAULT_PERSIST_IN_DMP                 = "false";
	private static final String DEFAULT_PERSIST_IN_FOLDER              = "true";
	private static final String DEFAULT_ENGINE_THREADS                 = "1";
	private static final String DEFAULT_PROTOTYPE_OUTPUT_DATA_MODEL_ID = "5fddf2c5-916b-49dc-a07d-af04020c17f7";
	private static final String DEFAULT_RESOURCE_WATCH_FOLDER_NAME     = "defaultresourcewatchfolder";
	private static final String DEFAULT_CONFIGURATION_FILE_NAME        = "oai-pmh-dc-xml-configuration.json";
	private static final String PROJECT_POSTFIX                        = "-project";

	private static final String PROJECT_ROOT = System.getProperty("project.root");
	private static final String USER_DIR     = System.getProperty("user.dir");
	private static final String ROOT_PATH;

	static {

		if (PROJECT_ROOT != null) {

			ROOT_PATH = PROJECT_ROOT;
		} else if (USER_DIR != null) {

			ROOT_PATH = USER_DIR;
		} else {

			TaskProcessingUnitTest.LOG.error("could not determine root path - project.root and user.dir is not available");

			ROOT_PATH = "";
		}
	}

	private static final String TEST_RESOURCES_ROOT_PATH =
			ROOT_PATH + File.separator + "src" + File.separator + "test" + File.separator + "resources";
	private static final String DEFAULT_RESULTS_FOLDER   = ROOT_PATH + File.separator + "target";

	private final String dmpAPIEndpoint;

	public TaskProcessingUnitTest() {

		final URL resource = Resources.getResource("tpu.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load tpu.properties", e);
		}

		dmpAPIEndpoint = properties.getProperty("dmp_api_endpoint", "http://localhost:8087/dmp/");

		TaskProcessingUnitTest.LOG.info("DMP API endpoint URI is = '{}'", dmpAPIEndpoint);
	}

	@Test
	public void testTPUExceptionAtXMLData() {

		final String testName = "TPU-test-1";
		final Properties config = generateDefaultConfig(testName + "-exception-at-xml-data");
		final String resourceWatchFolderName = "tputest1rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 1; XML document structures must start and end within the same entity.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	@Test
	public void testTPUWrongData() {

		final String testName = "TPU-test-2";
		final Properties config = generateDefaultConfig(testName + "-wrong-data");
		final String resourceWatchFolderName = "tputest2rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 1; Content is not allowed in prolog.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	@Test
	public void testTPUEmptyFile() {

		final String testName = "TPU-test-3";
		final Properties config = generateDefaultConfig(testName + "-empty-file");
		final String resourceWatchFolderName = "tputest3rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; Premature end of file.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	@Test
	public void testTPUEmptyFolder() {

		final String testName = "TPU-test-4";
		final Properties config = generateDefaultConfig(testName + "-empty-folder");
		final String resourceWatchFolderName = "tputest4rwf";

		final Path resourceWatchFolderPath = Paths.get(ROOT_PATH + File.separator + "target" + File.separator + resourceWatchFolderName);

		try {

			Files.createDirectory(resourceWatchFolderPath);
		} catch (final IOException e) {

			final String message = "couldn't create new folder for empty-folder test";

			TaskProcessingUnitTest.LOG.error(message, e);

			Assert.assertTrue(message, false);
		}

		final String configFileName = "xml-configuration.json";
		final String resourceWatchFolder = resourceWatchFolderPath.toString();
		final String configurationFilePath = TEST_RESOURCES_ROOT_PATH + File.separator + configFileName;

		TaskProcessingUnitTest.LOG.debug("[{}] resource watch folder = '{}'", testName, resourceWatchFolder);
		TaskProcessingUnitTest.LOG.debug("[{}] configuration file name = '{}'", testName, configurationFilePath);

		config.setProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER, resourceWatchFolder);
		config.setProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER, configurationFilePath);

		final String confFile = testName + "-dummy-config.properties";
		final String expectedErrorMessage = "could not determine files from watchfolder; there are no files in folder";

		TaskProcessingUnitTest.LOG.info("start " + testName);

		logConfig(config, testName);

		try {

			final String result = TaskProcessingUnit.startTPU(confFile, config);

			TaskProcessingUnitTest.LOG.debug("[{}] task execution result = '{}'", testName, result);
		} catch (final Exception e) {

			final String causeMessage = e.getMessage();

			TaskProcessingUnitTest.LOG.debug("[{}] actual error message '{}'", testName, causeMessage);

			Assert.assertTrue(causeMessage.startsWith(expectedErrorMessage));
		}

		TaskProcessingUnitTest.LOG.info("finished " + testName);
	}

	private void executeTPUTest(final String testName, final Properties config, final String resourceWatchFolderName, final String configFileName,
			final String expectedErrorMessage) {

		final String resourceWatchFolder = TEST_RESOURCES_ROOT_PATH + File.separator + resourceWatchFolderName;
		final String configurationFilePath = TEST_RESOURCES_ROOT_PATH + File.separator + configFileName;

		TaskProcessingUnitTest.LOG.debug("[{}] resource watch folder = '{}'", testName, resourceWatchFolder);
		TaskProcessingUnitTest.LOG.debug("[{}] configuration file name = '{}'", testName, configurationFilePath);

		config.setProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER, resourceWatchFolder);
		config.setProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER, configurationFilePath);

		final String confFile = testName + "-dummy-config.properties";
		executeTPUTest(testName, confFile, config, expectedErrorMessage);
	}

	private void executeTPUTest(final String testName, final String confFile, final Properties config, final String expectedErrorMessage) {

		TaskProcessingUnitTest.LOG.info("start " + testName);

		logConfig(config, testName);

		try {

			final String result = TaskProcessingUnit.startTPU(confFile, config);

			TaskProcessingUnitTest.LOG.debug("[{}] task execution result = '{}'", testName, result);
		} catch (final Exception e) {

			final Throwable cause1 = e.getCause();

			Assert.assertNotNull(cause1);

			final Throwable cause2 = cause1.getCause();

			Assert.assertNotNull(cause2);

			final Throwable cause3 = cause2.getCause();

			Assert.assertNotNull(cause3);

			final Throwable cause4 = cause3.getCause();

			Assert.assertNotNull(cause4);

			final String cause4Message = cause4.getMessage();

			TaskProcessingUnitTest.LOG.debug("[{}] actual error message '{}'", testName, cause4Message);

			Assert.assertTrue(cause4Message.startsWith(expectedErrorMessage));
		}

		TaskProcessingUnitTest.LOG.info("finished " + testName);
	}

	private void logConfig(final Properties config, final String testName) {

		final StringBuilder configSB = new StringBuilder();

		final Set<String> stringPropertyNames = config.stringPropertyNames();

		for (final String stringPropertyName : stringPropertyNames) {

			final String property = config.getProperty(stringPropertyName);

			configSB.append("\t'").append(stringPropertyName).append("' = '").append(property).append("'\n");
		}

		LOG.debug("[{}] result try to execute TPU task with following config\n{}", testName, configSB.toString());
	}

	private Properties generateDefaultConfig(final String testName) {

		final String defaultResourceWatchFolder = TEST_RESOURCES_ROOT_PATH + File.separator + DEFAULT_RESOURCE_WATCH_FOLDER_NAME;
		final String defaultConfigurationName = TEST_RESOURCES_ROOT_PATH + File.separator + DEFAULT_CONFIGURATION_FILE_NAME;

		TaskProcessingUnitTest.LOG.debug("default resource watch folder = '{}'", defaultResourceWatchFolder);
		TaskProcessingUnitTest.LOG.debug("default configuration file name = '{}'", defaultConfigurationName);

		final Properties config = new Properties();

		config.setProperty(TPUStatics.SERVICE_NAME_IDENTIFIER, testName);
		config.setProperty(TPUStatics.PROJECT_NAME_IDENTIFIER, testName + PROJECT_POSTFIX);
		config.setProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER, defaultResourceWatchFolder);
		config.setProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER, defaultConfigurationName);
		config.setProperty(TPUStatics.PROTOTYPE_PROJECT_ID_INDENTIFIER, DEFAULT_PROTOTYPE_PROJECT_ID);
		config.setProperty(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER, DEFAULT_PROTOTYPE_OUTPUT_DATA_MODEL_ID);
		config.setProperty(TPUStatics.DO_INIT_IDENTIFIER, DEFAULT_DO_INIT);
		config.setProperty(TPUStatics.DO_INITIAL_DATA_MODEL_INGEST_IDENTIFIER, DEFAULT_DO_INITIAL_DATA_MODEL_INGEST);
		config.setProperty(TPUStatics.ALLOW_MULTIPLE_DATA_MODELS_IDENTIFIER, DEFAULT_ALLOW_MULTIPLE_DATA_MODELS);
		config.setProperty(TPUStatics.DO_INGEST_IDENTIFIER, DEFAULT_DO_INGEST);
		config.setProperty(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER, DEFAULT_DO_TRANSFORMATIONS);
		config.setProperty(TPUStatics.DO_INGEST_ON_THE_FLY_IDENTIFIER, DEFAULT_DO_INGEST_ON_THE_FLY);
		config.setProperty(TPUStatics.DO_EXPORT_ON_THE_FLY_IDENTIFIER, DEFAULT_DO_EXPORT_ON_THE_FLY);
		config.setProperty(TPUStatics.DO_EXPORT_IDENTIFIER, DEFAULT_DO_EXPORT);
		config.setProperty(TPUStatics.PERSIST_IN_DMP_IDENTIFIER, DEFAULT_PERSIST_IN_DMP);
		config.setProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER, DEFAULT_PERSIST_IN_FOLDER);
		config.setProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER, DEFAULT_RESULTS_FOLDER);
		config.setProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER, DEFAULT_ENGINE_THREADS);
		config.setProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER, dmpAPIEndpoint);

		return config;
	}

}
