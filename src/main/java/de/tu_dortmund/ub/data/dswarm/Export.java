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

import de.tu_dortmund.ub.data.util.TPUUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Export Task for Task Processing Unit for d:swarm
 *
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-20
 */
public class Export implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(Export.class);

	public static final String EXPORT_IDENTIFIER = "export";
	private final String exportDataModelID;
	private final Optional<String> optionalExportMimeType;
	private final Optional<String> optionalExportFileExtension;
	private final Properties config;

	public Export(final String exportDataModelIDArg,
	              final Optional<String> optionalExportMimeTypeArg,
	              final Optional<String> optionalExportFileExtensionArg,
	              final Properties config) {

		exportDataModelID = exportDataModelIDArg;
		optionalExportMimeType = optionalExportMimeTypeArg;
		optionalExportFileExtension = optionalExportFileExtensionArg;
		this.config = config;
	}

	@Override
	public String call() {

		final String serviceName = config.getProperty(TPUStatics.SERVICE_NAME_IDENTIFIER);

		LOG.info(String.format("[%s] Starting 'XML-Export (Task)' ...", serviceName));

		try {

			TPUUtil.initSchemaIndices(serviceName, config);

			// export and save to results folder
			exportDataModel(exportDataModelID, optionalExportMimeType, optionalExportFileExtension, serviceName);

			return null;
		} catch (final Exception e) {

			final String message = String
					.format("[%s] Exporting and saving datamodel '%s' failed with a %s", serviceName, exportDataModelID, e.getClass()
							.getSimpleName());
			LOG.error(message, e);

			throw new RuntimeException(message, e);
		}
	}

	/**
	 * export the data model with the given ID in the given mime type
	 *
	 * @param dataModelID
	 * @throws Exception
	 */
	private void exportDataModel(final String dataModelID,
	                             final Optional<String> optionalExportMimeType,
	                             final Optional<String> optionalExportFileExtension,
	                             final String serviceName) throws Exception {

		try (final CloseableHttpClient httpclient = HttpClients.createDefault()) {

			final String engineDswarmAPI = config.getProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER);

			final String exportMimeType;

			if (optionalExportMimeType.isPresent()) {

				exportMimeType = optionalExportMimeType.get();
			} else {

				// application/xml as default mime type
				exportMimeType = APIStatics.APPLICATION_XML_MIMETYPE;
			}

			final String uri =
					engineDswarmAPI + DswarmBackendStatics.DATAMODELS_ENDPOINT + APIStatics.SLASH + dataModelID + APIStatics.SLASH + EXPORT_IDENTIFIER
							+ APIStatics.QUESTION_MARK + DswarmBackendStatics.FORMAT_IDENTIFIER + APIStatics.EQUALS
							+ exportMimeType;
			final HttpGet httpGet = new HttpGet(uri);

			//			httpGet.setHeader(name, value);

			LOG.info(String.format("[%s] dataModelID : %s", serviceName, dataModelID));
			LOG.info(String.format("[%s] request : %s", serviceName, httpGet.getRequestLine()));

			try (final CloseableHttpResponse httpResponse = httpclient.execute(httpGet)) {

				final int statusCode = httpResponse.getStatusLine().getStatusCode();

				switch (statusCode) {

					case 200: {

						LOG.info(String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine().getReasonPhrase()));

						break;
					}
					default: {

						LOG.error(String.format("[%s] %d : %s", serviceName, statusCode, httpResponse.getStatusLine().getReasonPhrase()));

						final String response = TPUUtil.getResponseMessage(httpResponse);

						throw new Exception("something went wrong at data model export: " + response);
					}
				}

				final String exportFileExtension;

				if (optionalExportFileExtension.isPresent()) {

					exportFileExtension = optionalExportFileExtension.get();
				} else {

					// xml as default file extension
					exportFileExtension = TaskProcessingUnit.XML_FILE_ENDING;
				}

				TPUUtil.writeResultToFile(httpResponse, config, exportDataModelID, exportFileExtension);
			}
		}
	}
}
