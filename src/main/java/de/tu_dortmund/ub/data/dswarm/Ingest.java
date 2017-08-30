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
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.json.Json;
import javax.json.JsonReader;

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ingest-Task for Task Processing Unit for d:swarm
 *
 * @author Dipl.-Math. Hans-Georg Becker (M.L.I.S.)
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-17
 *
 */
public class Ingest implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(Ingest.class);

	public static final String DATA_ENDPOINT                  = "data";
	public static final String FILE_IDENTIFIER                = "file";
	public static final String DELTA_UPDATE_FORMAT_IDENTIFIER = "delta";
	public static final String AMBERSENT                      = "&";
	public static final String ENABLE_VERSIONING_IDENTIFIER   = "enableVersioning";
	public static final String FALSE                          = "false";
	private final Properties config;

	private final String resource;
	private final String dataModelID;
	private final String resourceID;
	private final String projectName;
	private final int    cnt;

	public Ingest(final Properties config, final String resource, final String dataModelID, final String resourceID,
			final String projectName, final int cnt) {

		this.config = config;
		this.resource = resource;
		this.dataModelID = dataModelID;
		this.resourceID = resourceID;
		this.projectName = projectName;
		this.cnt = cnt;
	}

	//    @Override
	public String call() {

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);
		final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

		LOG.info(String.format("[%s] Starting 'Ingest (Task)' no. '%d' ...", serviceName, cnt));

		final String dataModelID = this.dataModelID;
		final String updateResourceID = resourceID;

		try {
			// build a InputDataModel for the resource
			final String name = String.format("resource for project '%s'", resource);
			final String description = String.format("'%s' - case %d", projectName, cnt);
			final String inputResourceJson = uploadFileAndUpdateResource(updateResourceID, resource, name,
					description, serviceName, engineDswarmAPI);
			final JsonReader jsonReader;

			if (inputResourceJson == null) {

				LOG.error("something went wrong at resource update");

				return null;
			}

			jsonReader = Json.createReader(IOUtils.toInputStream(inputResourceJson, APIStatics.UTF_8));
			final String inputResourceID = jsonReader.readObject().getString(DswarmBackendStatics.UUID_IDENTIFIER);
			LOG.info(String.format("[%s] inputResourceID = %s", serviceName, inputResourceID));

			if (inputResourceID != null) {

				// update the datamodel (will use it's (update) resource)
				updateDataModelContent(dataModelID, serviceName, engineDswarmAPI);

				// we don't need to transform after each ingest of a slice of records,
				// so transform and export will be done separately
				LOG.info(String.format("[%s] (Note: Only ingest, but no transformation or export done.)", serviceName));
			}

			// no need to clean up resources or datamodels anymore

			LOG.info(String.format("[%s] Finished 'Ingest (Task)' no. '%d' ...", serviceName, cnt));

			return null;
		} catch (final Exception e) {

			final String message = String.format("[%s] Processing resource '%s' failed with a %s", serviceName, resource,
					e.getClass().getSimpleName());

			LOG.error(message, e);

			throw new RuntimeException(message, e);
		}
	}

	/**
	 * update the datamodel with the given ID
	 *
	 * @param inputDataModelID
	 * @return
	 * @throws Exception
	 */
	private String updateDataModelContent(final String inputDataModelID, final String serviceName, final String engineDswarmAPI) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {
			// Update the existing input Data Model (we are simply using the example data model here ... TODO !)
			// note: format=delta query parameter must be set to ensure that existing records won't be deprecated in the datahub
			// note: enableVersioning=false to speed up ingest (however this requires unique resources)
			final String uri = engineDswarmAPI + DswarmBackendStatics.DATAMODELS_ENDPOINT + APIStatics.SLASH + inputDataModelID + APIStatics.SLASH
					+ DATA_ENDPOINT + APIStatics.QUESTION_MARK + DswarmBackendStatics.FORMAT_IDENTIFIER
					+ APIStatics.EQUALS + DELTA_UPDATE_FORMAT_IDENTIFIER + AMBERSENT + ENABLE_VERSIONING_IDENTIFIER + APIStatics.EQUALS + FALSE;
			final HttpPost httpPost = new HttpPost(uri);

			LOG.info(String.format("[%s] inputDataModelID : %s", serviceName, inputDataModelID));
			LOG.info(String.format("[%s] request : %s", serviceName, httpPost.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPost)) {

				int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				switch (statusCode) {

					case 200: {

						LOG.info(message);

						break;
					}
					default: {

						LOG.error(message);

						final String response = TPUUtil.getResponseMessage(httpResponse);

						throw new Exception("something went wrong at data model export: " + message + " " + response);
					}
				}
			}
		}

		return inputDataModelID;
	}

	/**
	 * upload a file and update an existing resource with it
	 *
	 * @param resourceUUID
	 * @param filename
	 * @param name
	 * @param description
	 * @return responseJson
	 * @throws Exception
	 */
	private String uploadFileAndUpdateResource(final String resourceUUID, final String filename, final String name, final String description,
			final String serviceName, final String engineDswarmAPI) throws Exception {

		if (null == resourceUUID)
			throw new Exception("ID of the resource to update was null.");

		final String resourceWatchFolder = config.getProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER);
		final String completeFileName = resourceWatchFolder + File.separatorChar + filename;

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final HttpPut httpPut = new HttpPut(engineDswarmAPI + DswarmBackendStatics.RESOURCES_ENDPOINT + APIStatics.SLASH + resourceUUID);

			final File file1 = new File(completeFileName);
			final FileBody fileBody = new FileBody(file1);
			final StringBody stringBodyForName = new StringBody(name, ContentType.TEXT_PLAIN);
			final StringBody stringBodyForDescription = new StringBody(description, ContentType.TEXT_PLAIN);

			final HttpEntity reqEntity = MultipartEntityBuilder.create()
					.addPart(DswarmBackendStatics.NAME_IDENTIFIER, stringBodyForName)
					.addPart(DswarmBackendStatics.DESCRIPTION_IDENTIFIER, stringBodyForDescription)
					.addPart(FILE_IDENTIFIER, fileBody)
					.build();

			httpPut.setEntity(reqEntity);

			LOG.info(String.format("[%s] request : %s", serviceName, httpPut.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpPut)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				final String message = String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine()
						.getReasonPhrase());

				final String response = TPUUtil.getResponseMessage(httpResponse);

				switch (statusCode) {

					case 200: {

						LOG.info(message);

						LOG.debug(String.format("[%s] responseJson : %s", serviceName, response));

						return response;
					}
					default: {

						LOG.error(message);

						throw new Exception("something went wrong at data model export: " + message + " " + response);
					}
				}
			}
		}
	}
}
