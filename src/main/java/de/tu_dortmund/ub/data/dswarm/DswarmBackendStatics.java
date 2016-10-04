/**
 * Copyright (C) 2015 – 2016 Dortmund University Library, SLUB Dresden & Avantgarde Labs GmbH (<code@dswarm.org>)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.tu_dortmund.ub.data.dswarm;

/**
 * @author tgaengler
 */
public final class DswarmBackendStatics {

	public static final String DATAMODELS_ENDPOINT     = "datamodels";
	public static final String RESOURCES_ENDPOINT      = "resources";
	public static final String CONFIGURATIONS_ENDPOINT = "configurations";
	public static final String PROJECTS_ENDPOINT       = "projects";
	public static final String SCHEMAS_ENDPOINT        = "schemas";
	public static final String TASKS_ENDPOINT          = "tasks";
	public static final String FILTERS_ENDPOINT        = "filters";

	public static final String UUID_IDENTIFIER              = "uuid";
	public static final String MAPPINGS_IDENTIFIER          = "mappings";
	public static final String DATA_RESOURCE_IDENTIFIER     = "data_resource";
	public static final String SCHEMA_IDENTIFIER            = "schema";
	public static final String NAME_IDENTIFIER              = "name";
	public static final String DESCRIPTION_IDENTIFIER       = "description";
	public static final String TASK_IDENTIFIER              = "task";
	public static final String JOB_IDENTIFIER               = "job";
	public static final String INPUT_DATA_MODEL_IDENTIFIER  = "input_data_model";
	public static final String OUTPUT_DATA_MODEL_IDENTIFIER = "output_data_model";
	public static final String SKIP_FILTER_IDENTIFIER       = "skip_filter";

	public static final String FORMAT_IDENTIFIER = "format";

	public static final String PERSIST_IDENTIFIER                 = "persist";
	public static final String DO_NOT_RETURN_DATA_IDENTIFIER      = "do_not_return_data";
	public static final String DO_INGEST_ON_THE_FLY               = "do_ingest_on_the_fly";
	public static final String DO_VERSIONING_ON_RESULT_IDENTIFIER = "do_versioning_on_result";
	public static final String UTILISE_EXISTING_INPUT_IDENTIFIER  = "utilise_existing_input_schema";
	public static final String DO_DATA_MODEL_INGEST_IDENTIFIER    = "doIngest";

	public static final String RESOURCE_ATTRIBUTES_IDENTIFIER            = "resource_attributes";
	public static final String PATH_IDENTIFIER                           = "path";
	public static final String PARAMETERS_IDENTIFIER                     = "parameters";
	public static final String STORAGE_TYPE_IDENTIFIER                   = "storage_type";
	public static final String XML_STORAGE_TYPE                          = "xml";
	public static final String MABXML_STORAGE_TYPE                       = "mabxml";
	public static final String MARCXML_STORAGE_TYPE                      = "marcxml";
	public static final String PNX_STORAGE_TYPE                          = "pnx";
	public static final String OAI_PMH_DC_ELEMENTS_STORAGE_TYPE          = "oai-pmh+dce";
	public static final String OAI_PMH_DCE_AND_EDM_ELEMENTS_STORAGE_TYPE = "oai-pmh+dce+edm";
	public static final String OAIPMH_DC_TERMS_STORAGE_TYPE              = "oai-pmh+dct";
	public static final String OAIPMH_MARCXML_STORAGE_TYPE               = "oai-pmh+marcxml";
}
