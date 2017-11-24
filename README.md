---

![TU Dortmund Logo](http://www.ub.tu-dortmund.de/images/tu-logo.png)

![UB Dortmund Logo](http://www.ub.tu-dortmund.de/images/ub-schriftzug.jpg)

---
([Dortmund University Library](https://www.ub.tu-dortmund.de/) in cooperation with [SLUB Dresden](http://slub-dresden.de) + [Avantgarde Labs](http://avantgarde-labs.de))

# Task Processing Unit for [D:SWARM](http://dswarm.org)

[![Join the chat at https://gitter.im/dswarm/dswarm](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/dswarm/dswarm?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

The task processing unit (TPU) is intended to process large amounts of data via [tasks](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#task) that make use of [mappings](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#mapping) that you have prepared and tested with the [D:SWARM backoffice webgui](https://github.com/dswarm/dswarm-documentation/wiki/Overview). So it can act as the production unit for D:SWARM, while the backoffice acts as development and/or testing unit (on smaller amounts of data).

The TPU acts as client by calling the HTTP API of the [D:SWARM backend](https://github.com/dswarm/dswarm) (see also [HTTP API discovery](https://github.com/dswarm/dswarm-documentation/wiki/HTTP-API-discovery)).

## TPU Task

A TPU task can consist of three parts, while each part can be optional. These are:
* ```ingest```: transforms data from a [data resource](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-resource) (of a certain data format, e.g., XML, JSON or CSV) with the help of a [configuration](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#configuration) into a [data model](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-model) that makes use of a [generic data format](https://github.com/dswarm/dswarm-documentation/wiki/Graph-Data-Model) (so that it can be consumed by the [transformation engine](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#transformation-engine) of D:SWARM)
* ```transform```: transforms data from an input data model via a task (which refers to a [job](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#job)) into an output data model
* ```export```: serializes data from a data model (usually an output data model) in the data hub into a certain data format, e.g., XML or various RDF serializations (currently, following mime types are supported: 'application/xml', 'text/turtle', 'application/trig', 'application/n-quads', 'application/rdf+xml' and 'text/n3')

## Processing Scenarios

The task processing unit can be configured for various scenarios, e.g.,
* ```ingest``` (only; persistent in the [data hub](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#data-hub))
* ```export``` (only; from data in the data hub)
* ```ingest``` (persistent), ```transform```, ```export``` (from persistent result)
* ```on-the-fly transform``` a.k.a. [*Streaming variant*](https://github.com/dswarm/dswarm-documentation/wiki#dswarm-streaming-variant) (input data will be ingested (/generated) on-the-fly + export data will be directly returned from the transformation result, without storing it in the data hub)
* any combination of the previous scenarios ;)

The fastest scenario is ```on-the-fly transform```, since it doesn't store anything in the data hub and does only the pure data processing. So it's recommend for data transformation scenarios, where only the output is important, but not the archiving of the data. Currently, this scenario supports following mime types for export: 
* 'application/json' ([Simple JSON](https://github.com/dswarm/dswarm-documentation/wiki/JSON-Flavours#simple-json))
* 'application/gdm+json' ([GDM JSON](https://github.com/dswarm/dswarm-documentation/wiki/JSON-Flavours#gdm-json))
* 'application/gdm+compact+json' ([GDM Ordered JSON](https://github.com/dswarm/dswarm-documentation/wiki/JSON-Flavours#gdm-ordered-json))
* 'application/gdm+simple+json' ([GDM Simple JSON](https://github.com/dswarm/dswarm-documentation/wiki/JSON-Flavours#gdm-simple-json))
* 'application/gdm+simple+short+json' ([GDM Simple Short JSON](https://github.com/dswarm/dswarm-documentation/wiki/JSON-Flavours#gdm-simple-short-json))
* 'application/jsc+json' (simple JSON that should conform an underlying JSON Schema)
* 'application/jsc+ldj' (simple JSON that should conform an underlying JSON Schema and where each record is written in a separate line (ldj = [line delimited JSON](https://en.wikipedia.org/wiki/JSON_Streaming#Line_delimited_JSON)))
* 'application/xml', 
* 'application/solr+update+xml' ([Solr Update Format](https://wiki.apache.org/solr/UpdateXmlMessages)), 
* 'text/turtle' ([Turtle](https://www.w3.org/TR/turtle/)), 
* 'application/trig', 
* 'application/trix', 
* 'application/n-quads' ([N-Quads](https://www.w3.org/TR/n-quads/)), 
* 'application/n-triples' ([N-Triples](https://www.w3.org/TR/n-triples/)) and 
* 'application/rdf+thrift' ([RDF Thrift](http://afs.github.io/rdf-thrift/)). 

The ```on-the-fly transform``` scenario can easily be parallelized via splitting your input data resource into several parts. Then each part can be processed in parallel.

## Requirements

For a (complete) TPU task execution you need to provide (at least):
* a [metadata repository](https://github.com/dswarm/dswarm-documentation/wiki/Glossary#metadata-repository) that contains the projects with the mappings that you would like to include into your task
* the data resource(s) that should act as input for your task execution
* the output data model (schema) to which the data should be mapped to (usually this can be the same as the one utilized in the mapping projects)

## Configuration

You can configure a TPU task with help of a properties file (`config.properties`). You don't need to configure each property for each processing scenario (maybe the properties will be simplified a bit in the future ;) ). Here is an overview of the configuration properties:

````
# this can be an arbitrary name
project.name=My-TPU-project

##############
# References #
##############

# this folder will be utilized when processing the input data resource into an input data model, i.e., put in here all data resources that should processed in your TPU task
resource.watchfolder=data/sources/resources

# the configuration that should be utilized to process the input data resource into an input data model
configuration.name=/home/user/conf/oai-pmh-marc-xml-configuration.json

# optional - only necessary, if init part is skipped
prototype.resourceID=Resource-f2b9e085-5b05-4853-ad82-06ce4fe1952d

# input data model id (optional - only necessary, if init part is skipped)
prototype.dataModelID=bbd368e8-b75c-0e64-b96a-ab812a700b4f

# optionally, only necessary, if transformation part is enabled
# (for legacy reason) if one project delivers all mappings for the tasks
prototype.projectID=819f2f6e-98ed-90e2-372e-71a0a1eec786

# if multiple projects deliver the mappings for the task
prototype.projectIDs=9d6ec288-f1bf-4f96-78f6-5399e3050125,69664ba5-bbe5-6f35-7a77-47bacf9d3731

# if an existing input schema should be utilised at input data model creation
prototype.inputSchemaID=Schema-cb8f4b96-9ab2-4972-88f8-143656199518

# the output data model refers to the output schema as well
prototype.outputDataModelID=DataModel-cf998267-392a-4d87-a33a-88dd1bffb016

# optional - a skip filter that should be applied at Job base (to skip records from processing)
prototype.skipFilterID=Filter-efada9c4-b48c-4f12-9408-14f5c6ed6ea4

##########
# Ingest #
##########

# enables init part (i.e., resource + data model creation)
init.do=true

# (optionally) enhance input data resources (currently, only for XML)
init.enhance_data_resource=true

# if disable, task.do_ingest_on_the_fly needs to enabled
init.data_model.do_ingest=false

# if enable, task.do_ingest_on_the_fly needs to be enabled
init.multiple_data_models=true

# enables ingest (i.e., upload of data resources + ingest into given data model (in the data hub)
ingest.do=true

#############
# Transform #
#############

# enables task execution (on the given data model with the given mappings into the given output data model)
transform.do=true

# to do `ingest on-the-fly` at task execution time, you need to disable the init and ingest part and provide a valid prototype dataModelID
task.do_ingest_on_the_fly=true

# to do `export on-the-fly` at task execution time, you need to disable results.persistInDMP (otherwise, it would be written to the data hub)
# + you need to disable export part (otherwise it would be exported twice)
task.do_export_on_the_fly=true

##########
# Export #
##########

# enables export from the data hub (from the given output data model)
export.do=true

# the mime type for the export (export on-the-fly or export from the data hub)
# currently possible mime types are 'application/xml', 'text/turtle', 'application/trig', 'application/trix', 'application/n-quads', 'application/n-triples' and 'application/rdf+thrift' for export on-the-fly
# and 'application/xml', 'text/turtle', 'application/trig', 'application/n-quads', 'application/rdf+xml' and 'text/n3' for export from the data hub
# default is 'application/xml'
export.mime_type=application/xml

###########
# Results #
###########

# (optionally) - only necessary, if transform part is enabled; i.e., task execution result will be stored in the data hub)
# + if export part is enabled, this needs to be enabled as well (otherwise it wouldn't find any data in the data hub for export)
results.persistInDMP=false

# needs to be enable if data is export to the file system (also necessary for export on-the-fly)
results.persistInFolder=true

# the folder where the transformation result or export should be stored
results.folder=data/target/results

# should be disabled, otherwise the task execution will return JSON
results.writeDMPJson=false

########################
# Task Processing Unit #
########################

# the number of threads that should be utilized for execution the TPU task in parallel
# currently, multi-threading can only be utilized for on-the-fly transform, i.e., init.do=true + init.data_model.do_ingest=false + init.multiple_data_models=true + ingest.do=false + transform.do=true +  task.do_ingest_on_the_fly=true + task.do_export_on_the_fly=true + export.do=false + results.persistInDMP=false
engine.threads=1

# the base URL of the D:SWARM backend API
engine.dswarm.api=http://example.com/dmp/

# the base URL of the D:SWARM graph extension
engine.dswarm.graph.api=http://example.com/graph/

````

## Execution

You can build the TPU with the following command (only required once, or when TPU code was updated):

````
mvn clean package -DskipTests
````

You can execute your TPU task with the following command:

	$JAVA_HOME/jre/bin/java -cp taskprocessingunit-1.0-SNAPSHOT-onejar.jar de.tu_dortmund.ub.data.dswarm.TaskProcessingUnit -conf=conf/config.properties
You need to ensure that at least the D:SWARM backend is running (+ optionally, the data hub and D:SWARM graph extension).  

## Logging

You can (usually) find logs of your TPU task executions in `[TPU HOME]/logs`.

## Example Configuration for On-The-Fly Transform Processing

The following configuration illustrates the property settings for a multi-threading ```on-the-fly transform``` processing scenario (i.e., input data ingest will be done on-the-fly before D:SWARM task execution + result export will be done immediately after the D:SWARM task execution):

```
service.name=deg-small-test-run
project.name=degsmalltest
resource.watchfolder=/data/source-data/DEG-small
configuration.name=/home/dmp/config/oai-pmh-marc-xml-configuration.json
prototype.projectIDs=9d6ec288-f1bf-4f96-78f6-5399e3050125,69664ba5-bbe5-6f35-7a77-47bacf9d3731
prototype.outputDataModelID=5fddf2c5-916b-49dc-a07d-af04020c17f7
init.do=true
init.data_model.do_ingest=false
init.multiple_data_models=true
ingest.do=false
transform.do=true
task.do_ingest_on_the_fly=true
task.do_export_on_the_fly=true
export.mime_type=application/xml
export.do=false
results.persistInDMP=false
results.persistInFolder=true
results.folder=/home/dmp/data/degsmalltest/results
engine.threads=10
engine.dswarm.api=http://localhost:8087/dmp/
engine.dswarm.graph.api=http://localhost:7474/graph/
```

For this scenario, the input data resource needs to be divided into multiple parts. Then each part will be executed as a separate TPU task (and produce a separate export file).
