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

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.stream.JsonGenerator;

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.commons.io.IOUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dswarm.xmlenhancer.XMLEnhancer;

/**
 * Init-Task for Task Processing Unit for d:swarm<br/>
 * creates a resource + configuration + data model
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-17
 *
 */
public class Init implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(Init.class);

	public static final String FILE_IDENTIFIER          = "file";
	public static final String CONFIGURATION_IDENTIFIER = "configuration";
	public static final String DATA_MODEL_ID            = "data_model_id";
	public static final String RESOURCE_ID              = "resource_id";
	public static final String CONFIGURATION_ID         = "configuration_id";

	private static final String JAVA_IO_TMPDIR = "java.io.tmpdir";
	private static final String OS_TEMP_DIR    = System.getProperty(JAVA_IO_TMPDIR);

	private final Properties config;
	private final String     initResourceFile;
	private final int        cnt;

	public Init(final String initResourceFile, final Properties config, final int cnt) {

		this.initResourceFile = initResourceFile;
		this.config = config;
		this.cnt = cnt;
	}

	//    @Override
	public String call() {

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);
		final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);
		final Optional<Boolean> optionalEnhanceInputDataResource = TPUUtil.getBooleanConfigValue(TPUStatics.ENHANCE_INPUT_DATA_RESOURCE, config);

		LOG.info(String.format("[%s][%d] Starting 'Init (Task)' ...", serviceName, cnt));

		try {

			final boolean doIngest;

			final String doIngestString = config.getProperty(TPUStatics.DO_INITIAL_DATA_MODEL_INGEST_IDENTIFIER);

			if (doIngestString != null && !doIngestString.trim().isEmpty()) {

				doIngest = Boolean.valueOf(doIngestString);
			} else {

				// default = true
				doIngest = true;
			}

			if (doIngest) {

				LOG.debug("[{}][{}] do data model creation with data ingest", serviceName, cnt);

				TPUUtil.initSchemaIndices(serviceName, config);
			}

			final String configurationFileName = config.getProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER);
			final String configurationJSONString = readFile(configurationFileName, StandardCharsets.UTF_8);
			final JsonObject configurationJSON = TPUUtil.getJsonObject(configurationJSONString);

			final String finalInputResourceFile;

			if (optionalEnhanceInputDataResource.isPresent() && Boolean.TRUE.equals(optionalEnhanceInputDataResource.get())) {

				final Optional<String> optionalUpdatedInputResourceFile = enhanceInputDataResource(initResourceFile, configurationJSON);

				if (optionalUpdatedInputResourceFile.isPresent()) {

					finalInputResourceFile = optionalUpdatedInputResourceFile.get();
				} else {

					finalInputResourceFile = initResourceFile;
				}
			} else {

				finalInputResourceFile = initResourceFile;
			}

			final String name = String.format("resource for project '%s'", initResourceFile);
			final String description = String.format("'resource does not belong to a project' - case %d", cnt);
			final String inputResourceJson = uploadFileAndCreateResource(finalInputResourceFile, name, description, serviceName, engineDswarmAPI);

			if (inputResourceJson == null) {

				final String message = "something went wrong at resource creation";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			final JsonObject inputResourceJSON = TPUUtil.getJsonObject(inputResourceJson);
			final String inputResourceID = inputResourceJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] input resource id = %s", serviceName, cnt, inputResourceID));

			if (inputResourceID == null) {

				final String message = "something went wrong at resource creation, no resource uuid available";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			// TODO: refactor this, so that a configuration only needs to be create once per TPU task
			// create configuration
			final String finalConfigurationJSONString = createConfiguration(configurationJSONString, serviceName, engineDswarmAPI);

			if (finalConfigurationJSONString == null) {

				final String message = "something went wrong at configuration creation";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			final JsonObject finalConfigurationJSON = TPUUtil.getJsonObject(finalConfigurationJSONString);
			final String configurationID = finalConfigurationJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] configuration id = %s", serviceName, cnt, configurationID));

			if (configurationID == null) {

				final String message = "something went wrong at configuration creation, no configuration uuid available";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			// check for existing input schema
			final Optional<JsonObject> optionalInputSchema = getInputSchema(serviceName, engineDswarmAPI);

			// create the datamodel (will use it's resource)
			final String dataModelName = String.format("data model %d", cnt);
			final String dataModelDescription = String.format("data model description %d", cnt);
			final String dataModelJSONString = createDataModel(inputResourceJSON, finalConfigurationJSON, optionalInputSchema, dataModelName,
					dataModelDescription, serviceName,
					engineDswarmAPI, doIngest);

			if (dataModelJSONString == null) {

				final String message = "something went wrong at data model creation";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			final JsonObject dataModelJSON = TPUUtil.getJsonObject(dataModelJSONString);
			final String dataModelID = dataModelJSON.getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s][%d] data model id = %s", serviceName, cnt, dataModelID));

			if (dataModelID == null) {

				final String message = "something went wrong at data model creation, no data model uuid available";

				LOG.error(message);

				throw new RuntimeException(message);
			}

			// we don't need to transform after each ingest of a slice of records,
			// so transform and export will be done separately
			LOG.info(String.format("[%s][%d] (Note: Only ingest, but no transformation or export done.)", serviceName, cnt));

			final StringWriter stringWriter = new StringWriter();
			final JsonGenerator jp = Json.createGenerator(stringWriter);

			jp.writeStartObject();
			jp.write(DATA_MODEL_ID, dataModelID);
			jp.write(RESOURCE_ID, inputResourceID);
			jp.write(CONFIGURATION_ID, configurationID);
			jp.writeEnd();

			jp.flush();
			jp.close();

			final String result = stringWriter.toString();

			stringWriter.flush();
			stringWriter.close();

			return result;
		} catch (final Exception e) {

			final String message = String.format("[%s][%d] Processing resource '%s' failed with a %s", serviceName, cnt, initResourceFile,
					e.getClass().getSimpleName());

			LOG.error(message, e);

			throw new RuntimeException(message, e);
		}
	}

	/**
	 * uploads a file and creates a data resource with it
	 *
	 * @param filename
	 * @param name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String uploadFileAndCreateResource(final String filename, final String name, final String description, final String serviceName,
			final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + DswarmBackendStatics.RESOURCES_ENDPOINT);

			final File file1 = new File(filename);
			final FileBody fileBody = new FileBody(file1);
			final StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
			final StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

			final HttpEntity reqEntity = MultipartEntityBuilder.create()
					.addPart(DswarmBackendStatics.NAME_IDENTIFIER, stringBodyForName)
					.addPart(DswarmBackendStatics.DESCRIPTION_IDENTIFIER, stringBodyForDescription)
					.addPart(FILE_IDENTIFIER, fileBody)
					.build();

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();
				final HttpEntity httpEntity = httpResponse.getEntity();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 201: {

						LOG.info(message);
						final StringWriter writer = new StringWriter();
						IOUtils.copy(httpEntity.getContent(), writer, APIStatics.UTF_8);
						final String responseJson = writer.toString();
						writer.flush();
						writer.close();

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, responseJson));

						return responseJson;
					}
					default: {

						LOG.error(message);

						EntityUtils.consume(httpEntity);

						throw new Exception("something went wrong at resource upload: " + message);
					}
				}
			}
		}
	}

	/**
	 * creates a configuration from a given configuration JSON
	 *
	 * @param configurationJSONString
	 * @return responseJson
	 * @throws Exception
	 */
	private String createConfiguration(final String configurationJSONString, final String serviceName,
			final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPost httpPost = new HttpPost(engineDswarmAPI + DswarmBackendStatics.CONFIGURATIONS_ENDPOINT);

			final StringEntity reqEntity = new StringEntity(configurationJSONString,
					ContentType.create(APIStatics.APPLICATION_JSON_MIMETYPE, Consts.UTF_8));

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				final String response = TPUUtil.getResponseMessage(httpResponse);

				switch (statusCode) {

					case 201: {

						LOG.info(message);

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, response));

						return response;
					}
					default: {

						LOG.error(message);

						throw new Exception("something went wrong at configuration creation: " + message + " " + response);
					}
				}
			}
		}
	}

	/**
	 * retrieves an existing input schema from a given schema identifier
	 *
	 * @return responseJson
	 * @throws Exception
	 */
	private Optional<JsonObject> getInputSchema(final String serviceName, final String engineDswarmAPI) throws Exception {

		final Optional<String> optionalInputSchemaID = TPUUtil.getStringConfigValue(TPUStatics.PROTOTYPE_INPUT_SCHEMA_ID_IDENTIFIER, config);

		if (!optionalInputSchemaID.isPresent()) {

			return Optional.empty();
		}

		final String inputSchemaID = optionalInputSchemaID.get();

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpGet httpGet = new HttpGet(engineDswarmAPI + DswarmBackendStatics.SCHEMAS_ENDPOINT + APIStatics.SLASH + inputSchemaID);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpGet.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				final String response = TPUUtil.getResponseMessage(httpResponse);

				switch (statusCode) {

					case 200: {

						LOG.info(message);

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, response));

						return Optional.ofNullable(TPUUtil.getJsonObject(response));
					}
					default: {

						LOG.error(message);

						throw new Exception("something went wrong at input schema retrieval: " + message + " " + message);
					}
				}
			}
		}
	}

	/**
	 * creates a data model from given resource + configuration JSON (+ optional input schema)
	 *
	 * @param resourceJSON
	 * @param configurationJSON
	 * @param optionalInputSchema
	 * @param name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String createDataModel(final JsonObject resourceJSON, final JsonObject configurationJSON, final Optional<JsonObject> optionalInputSchema,
			final String name, final String description, final String serviceName, final String engineDswarmAPI, final boolean doIngest)
			throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final String uri = engineDswarmAPI + DswarmBackendStatics.DATAMODELS_ENDPOINT + APIStatics.QUESTION_MARK
					+ DswarmBackendStatics.DO_DATA_MODEL_INGEST_IDENTIFIER + APIStatics.EQUALS + doIngest;

			final HttpPost httpPost = new HttpPost(uri);

			final StringWriter stringWriter = new StringWriter();
			final JsonGenerator jp = Json.createGenerator(stringWriter);

			jp.writeStartObject();
			jp.write(DswarmBackendStatics.NAME_IDENTIFIER, name);
			jp.write(DswarmBackendStatics.DESCRIPTION_IDENTIFIER, description);
			jp.write(CONFIGURATION_IDENTIFIER, configurationJSON);
			jp.write(DswarmBackendStatics.DATA_RESOURCE_IDENTIFIER, resourceJSON);

			if (optionalInputSchema.isPresent()) {

				LOG.info("[{}][{}] add existing input schema to input data model", serviceName, cnt);

				jp.write(DswarmBackendStatics.SCHEMA_IDENTIFIER, optionalInputSchema.get());
			}

			jp.writeEnd();

			jp.flush();
			jp.close();

			final StringEntity reqEntity = new StringEntity(stringWriter.toString(),
					ContentType.create(APIStatics.APPLICATION_JSON_MIMETYPE, Consts.UTF_8));

			stringWriter.flush();
			stringWriter.close();

			httpPost.setEntity(reqEntity);

			LOG.info(String.format("[%s][%d] request : %s", serviceName, cnt, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s][%d] %d : %s", serviceName, cnt, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				final String response = TPUUtil.getResponseMessage(httpResponse);

				switch (statusCode) {

					case 201: {

						LOG.info(message);

						LOG.debug(String.format("[%s][%d] responseJson : %s", serviceName, cnt, response));

						return response;
					}
					default: {

						LOG.error(message);

						throw new Exception("something went wrong at data model creation: " + message + " " + response);
					}
				}
			}
		}
	}

	private static String readFile(String path, Charset encoding) throws IOException {

		final byte[] encoded = Files.readAllBytes(Paths.get(path));

		return new String(encoded, encoding);
	}

	private Optional<String> enhanceInputDataResource(final String inputDataResourceFile, final JsonObject configurationJSON) throws Exception {

		final JsonObject parameters = configurationJSON.getJsonObject(DswarmBackendStatics.PARAMETERS_IDENTIFIER);

		if (parameters == null) {

			LOG.debug("could not find parameters in configuration '{}'", configurationJSON.toString());

			return Optional.empty();
		}

		final String storageType = parameters.getString(DswarmBackendStatics.STORAGE_TYPE_IDENTIFIER);

		if (storageType == null || storageType.trim().isEmpty()) {

			LOG.debug("could not find storage in parameters of configuration '{}'", configurationJSON.toString());

			return Optional.empty();
		}

		switch (storageType) {

			case DswarmBackendStatics.XML_STORAGE_TYPE:
			case DswarmBackendStatics.MABXML_STORAGE_TYPE:
			case DswarmBackendStatics.MARCXML_STORAGE_TYPE:
			case DswarmBackendStatics.PNX_STORAGE_TYPE:
			case DswarmBackendStatics.OAI_PMH_DC_ELEMENTS_STORAGE_TYPE:
			case DswarmBackendStatics.OAI_PMH_DCE_AND_EDM_ELEMENTS_STORAGE_TYPE:
			case DswarmBackendStatics.OAIPMH_DC_TERMS_STORAGE_TYPE:
			case DswarmBackendStatics.OAIPMH_MARCXML_STORAGE_TYPE:

				// only XML is supported right now

				break;
			default:

				LOG.debug("storage type '{}' is currently not supported for input data resource enhancement", storageType);

				return Optional.empty();
		}

		final Path inputDataResourcePath = Paths.get(inputDataResourceFile);

		final Path inputDataResourceFileNamePath = inputDataResourcePath.getFileName();
		final String inputDataResourceFileName = inputDataResourceFileNamePath.toString();
		final String newInputDataResourcePath = OS_TEMP_DIR + File.separator + inputDataResourceFileName;

		LOG.debug("try to enhance input data resource '{}'", inputDataResourceFile);

		XMLEnhancer.enhanceXML(inputDataResourceFile, newInputDataResourcePath);

		LOG.debug("enhanced input data resource for '{}' can be found at ''{}", inputDataResourceFile, newInputDataResourcePath);

		return Optional.of(newInputDataResourcePath);
	}
}
