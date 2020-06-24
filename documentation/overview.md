# Using the Spark CDM Connector

***Limited preview release*** <br/>
Guide last updated, May 15, 2020

## Overview

The Spark CDM Connector enables a Spark program to read and write CDM entities in a CDM folder via dataframes. In principle, the Spark CDM Connector will work in any Spark environment, however this limited preview release has only been tested with and is only supported with Azure Databricks
and Apache Spark in Azure Synapse. 

**During this limited preview, use of the Spark CDM Connector in production applications is not recommended or supported.** 

**The connector capabilities and API may be changed without notice.**

For information on defining CDM documents using CDM 1.0 see 
[https://docs.microsoft.com/en-us/common-data-model/](https://docs.microsoft.com/en-us/common-data-model/)

## Installing the Spark CDM connector
**Azure Databricks:** the Spark CDM connector library is provided as a jar file in GitHub and Maven that must be installed in an Azure Databricks cluster. 
[https://mvnrepository.com/artifact/com.microsoft.azure/spark-cdm-connector](https://mvnrepository.com/artifact/com.microsoft.azure/spark-cdm-connector)\
[https://github.com/Azure/spark-cdm-connector](https://github.com/Azure/spark-cdm-connector)

**Apache Spark for Azure Synapse:** the Spark CDM Connector is pre-installed and requires no additional installation. 

Note that the latest version of the connector may not be installed in Synapse at any point in time.  

Use the API below to retrieve the current version of the Spark CDM Connector and compare with the release notes in GitHub.
```
com.microsoft.cdm.BuildInfo.version
```
Once installed, sample code and CDM models are provided in [GitHub](https://github.com/Azure/spark-cdm-connector/tree/master/samples).

## Scenarios
### Supported scenarios

The following scenarios are supported:  
- Reading data from an entity in a CDM folder into a Spark dataframe.
- Writing from a Spark dataframe to an entity in a CDM folder based on a CDM entity definition.
- Writing from a Spark dataframe to an entity in a CDM folder based on the dataframe schema.

### Capabilities/limitations

The following capabilities or limitations apply:
- Supports CDM folders in ADLS gen2 with HNS enabled only.
- Supports reading from CDM folders described by either manifest or model.json files.
- Supports writing to CDM folders described by a manifest file only. Write support for model.json file is not planned.
- Supports data in CSV and Apache Parquet format. Support for nested parquet files is planned.
- Supports partition patterns on read and sub-manifests on read and write.

See also, Known issues, below.

### Unsupported scenarios

The following scenarios are not supported:

- Programmatic access to entity metadata after reading an entity.
- Programmatic access to set or override metadata when writing an entity.
- Schema drift - where data in a dataframe being written includes additional attributes not included in the entity definition
- Schema evolution - where entity partitions reference alternate earlier versions of the entity definition 

## Using the Spark CDM connector to read and write CDM data

The Spark CDM connector is used to modify normal Spark dataframe read and write behavior, with a series of options and modes used as described below.

### Reading data

When reading data, the library uses metadata in the CDM folder to create the dataframe based on the structure of the source entity. Attribute names are used as column names and attribute datatypes are mapped to the column datatype. When the dataframe is loaded it is populated from the partitions identified in the manifest.

Partitions for any given entity can be in different formats, for example, a mix of CSV and parquet files. Regardless of format, all the entity data files identified in the manifest are combined into one dataset and loaded to the dataframe.

### Writing Data

When writing to a CDM folder, if the entity does not already exist in the CDM folder, a new entity and definition is created and added to the CDM folder and referenced in the manifest. Two writing modes are supported:

**Explicit write**: the physical entity definition is based on a reference to an entity that you specify.
- If the dataframe structure does not match the referenced entity definition, an error is returned.  Ensure that attribute datatypes match fields in the dataframe, including precision and scale which is set via traits.
- If the dataframe is valid -
  - If the entity already exists in the manifest, the provided entity definition is
resolved and validated against the definition in the CDM folder. If the
definitions do not match an error is returned, otherwise data is written.
  - If the entity does not exist in the CDM folder, a resolved copy of the entity
definition is written to the CDM folder and data is written.

**Implicit write**: the entity definition is derived from the dataframe structure.
- If the entity does not exist in the CDM folder, the implicit definition is used to create the resolved entity definition in the target CDM folder.
- If the entity exists in CDM folder, the implicit definition is validated against the existing entity definition. If the definitions do not match an error is returned, otherwise data is written.
- In addition, a derived logical entity definition is written into a subfolder of the entity folder

Data is written to data subfolder(s) within an entity subfolder subject to a save mode. The save mode determines whether the new data overwrites or is appended to existing data, or an error is returned if data exists. The default is to return an error if data already exists.

### Parameters, options and save mode

For both read and write, the connector library name is provided as a parameter. A series of options are used to parameterize the behavior of the Spark CDM connector. When writing, a save mode is also supported.

The connector library name, options and save mode are formatted as follows:

- dataframe.read.format("com.microsoft.cdm") [.option("option", "value")]*
- dataframe.write.format("com.microsoft.cdm") [.option("option", "value")]* .mode(mode)

Here's an example of how the connector is used for read, showing some of the options.  There are more examples later.
```scala
val readDf = spark.read.format("com.microsoft.cdm")
  .option("storage", "mystorageaccount.dfs.core.windows.net")
  .option("container", "customerleads")
  .option("manifest", "default.manifest.cdm.json")
  .option("entity", "Customer")
  .option("appId", "<application id>")
  .option("appKey", "<application key>")
  .option("tenantId", "<tenantid>")
  .load()
```
#### Credential options

Credentials must be provided for the Spark CDM connector to access data. In Azure Active Directory, create an App Registration and then grant this App Registration access to your storage account using either of the following roles: **Storage Blob Data Contributor** to allow the library to write to CDM folders, or **Storage Blob Data Reader** to allow only read. 

Once permissions are created, you can pass the app id, app key, and tenant id to the connector. It is recommended to use Azure Key Vault to secure these values, to ensure they are not written in clear text in a notebook file.

In Azure Databricks, create a secret scope which can be backed by Azure Key Vault. See:
[https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azurekey-vault-backed-secret-scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#create-an-azurekey-vault-backed-secret-scope)

| **Option**   |**Description**  |**Pattern and example usage**  |
|----------|---------|:---------:|
| appId | The app registration ID used to authenticate to the storage account | \<guid\> |
| appKey | The registered app key or secret | \<encrypted secret\> |
| tenantId | The Azure Active Directory tenant ID under which the app is registered.      | \<guid\>  |

#### Common options

The following options identify the entity in the CDM folder that is either being read or written to.

**IMPORTANT** Folder and file names in options should be URL-encoded, for example, use %20 in place of a space.

|**Option**  |**Description**  |**Pattern and example usage**  |
|---------|---------|:---------:|
|storage|An ADLS gen2 storage account *with HNS enabled* in which the CDM folder is located.  Be sure to use the dfs.core.windows.net URL | \<accountName\>.dfs.core.windows.net "myAccount.dfs.core.windows.net"        |
|container|An ADLS gen2 file system / container in which the source or target CDM folder is located | \<containerName\> <br/>"myContainer"        |
|manifest|The relative path to the manifest or model.json file from the container root. For read, can be a root manifest or a sub-manifest. For write, must be the root manifest.|[\<folderPath\>]/fullManifestName, "default.manifest.cdm.json" "employees.manifest.cdm.json" <br/> "employees/person.manifest.cdm.json" "employees/model.json" (read only)         |
|entity| The name of the source or target entity in the manifest. When writing an entity for the first time in a folder, the resolved entity definition will be given this name. | \<entityName\> <br/>"customer"|

#### Entity definition options

**Explicit Write:** The following options are used when an explicit entity definition is used to define and
validate the entity being written. If the dataframe schema does not match the dataframe schema on
write, an error is reported.

**Implicit Write:** If the entityDefinition options are not supplied when creating an entity in a CDM folder,
the entity structure is defined by the schema of the dataframe and basic CDM metadata will be created.

**Read:** The entityDefinitionContainer (if different from container) and entityDefinitionModelRoot, or
useCdmGithubModelRoot are required to allow resolution of  the logical entity definition from which the
physical entity in the CDM folder being read was resolved.

|**Option**  |**Description**  |**Pattern / example usage**  |
|---------|---------|:---------:|
|useCdmGithubModelRoot | Explicit write only. Indicates the source model root is located at [https://github.com/microsoft/CDM/tree/master/schemaDocuments](https://github.com/microsoft/CDM/tree/master/schemaDocuments) <br/>Used to reference entity types defined in the CDM GitHub.<br/>***Overrides:*** entityDefinitionStorage, entityDefinitionContainer, entityDefinitionModelRoot if specified| "useCdmGithubModelRoot" |
|useCdmGithub|Sets the modelroot alias to [https://github.com/microsoft/CDM/tree/master/schemaDocuments](https://github.com/microsoft/CDM/tree/master/schemaDocuments) to resolve the reference to foundations.cdm.json. <br/>**Required for Implicit Write and Read  only.** |"useCdmGithub" |
|*entityDefinitionStorage [NOT YET SUPPORTED]*|*The ADLS gen2 storage account containing the entity definition. Required if different to the storage account hosting the CDM folder.*|*\<accountName\>.dfs.core.windows.net*|*"[myAccount.dfs.core.windows.net]"*|
|entityDefinitionContainer|The storage container containing the entity definition. Required if different to the CDM folder container.| \<containerName><br/> "models"|
|entityDefinitionModelRoot|The location of the model root or corpus within the container. If not specified, defaults to the root folder.|\<folderPath\> <br/> "crm/core"<br/> (use "/" for the root folder of the container)|
|entityDefinition|Location of the entity. File path to the CDM definition file relative to the model root, including the name of the entity in that file.|\<folderPath\>/\<entityName\>.cdm.json/\<entityName\><br/>"sales/customer.cdm.json/customer"|

In the example above, the full path to the customer entity definition object is ```
https://myAccount.dfs.core.windows.net/models/crm/core/sales/customer.cdm.json/customer```, where
‘models’ is the container in ADLS.

**IMPORTANT:** Reading an entity, where the physical entity definition references (imports) its logical definition, requires the entityDefinitionContainer (if different from container) and entityDefinitionModelRoot (for the logical entity definition) to be specified.

#### Folder structure and data format options on write

The folder organization and file format used on write can be changed with the following options.

|**Option**  |**Description**  |**Pattern / example usage**  |
|---------|---------|:---------:|
|useSubManifest|If true, causes the target entity to be included in the root manifest via a sub-manifest. The sub-manifest and the entity definition are written into an entity folder beneath the root. Default is false.|"true" or "false" |
|format|Defines the file format. Current supported file formats are CSV and parquet. Default is "csv"|"csv" or "parquet" <br/> |
|compression|Defines the compression format used with parquet. Default is "snappy"|"uncompressed" \| "snappy" \| "gzip" \| "lzo". <br/> See note below on using lzo with Azure Databricks

Note that the lzo codec is not available by default in Azure Databricks but must be installed. See
[https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-lzo](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/read-lzo)

By default, data is written as CSV into a default Data subfolder within the target entity folder.

Options to use and control the partition pattern, and to provide CSV options are not yet supported.

#### Save Mode

The save mode specifies how existing entity data in the CDM folder is handled. Options are to
overwrite, append to, or error if data already exists. The default save mode is ErrorIfExists

|**Mode**  |**Description**|
|---------|---------|
|SaveMode.Overwrite |Will overwrite existing partitions with data being written.<br/>Note: overwrite does not support changing the schema; if the schema of the data being written is incompatible with the existing entity definition an error will be thrown. |
|SaveMode.Append |Will append data being written as new partitions alongside the existing partitions.<br/>Note: append does not support changing the schema; if the schema of the data being written is incompatible with the existing entity definition an error will be thrown.|
|SaveMode.ErrorIfExists|Will return an error if partitions already exist.|

See *Folder organization* below for details of how data files are organized on write.

### Examples

The following examples all use appId, appKey and tenantId variables initialized earlier in the code based on an Azure app registration that has been given Storage Blob Data Contributor permissions on the storage for write and Storage Blob Data Reader permissions for read.

#### Implicit Write – using dataframe schema only

This code writes the dataframe df to a CDM folder with a manifest at
[https://mystorage.dfs.core.windows.net/cdmdata/Contacts/default.manifest.cdm.json](https://mystorage.dfs.core.windows.net/cdmdata/Contacts/default.manifest.cdm.json) with an Event entity.

Event data is written as parquet files, compressed with gzip, that are appended to the folder (new files
are added without deleting existing files).

```scala

df.write.format("com.microsoft.cdm")
 .option("storage", "mystorage.dfs.core.windows.net")
 .option("container", "cdmdata")
 .option("manifest", "/Contacts/default.manifest.cdm.json")
 .option("entity", "Event")
 .option("format", "parquet")
 .option("compression", "gzip")
 .option("appId", appid)
 .option("appKey", appkey)
 .option("tenantId", tenantid)
 .mode(SaveMode.Append)
 .save()
```

#### Explicit Write - using an entity defined in the CDM GitHub

This code writes the dataframe df to a CDM folder with the manifest at [https://mystorage.dfs,core.windows.net/cdmdata/Teams/root.manifest.cdm.json](https://mystorage.dfs,core.windows.net/cdmdata/Teams/root.manifest.cdm.json) and a sub-manifest containing the TeamMembership entity, created in a TeamMembership subdirectory. TeamMembership data is written as CSV files (by default) that overwrite any existing data files. The entity definition is retrieved from the CDM GitHub repo, at:
[https://github.com/microsoft/CDM/tree/master/schemaDocuments/core/applicationCommon/TeamMembership.cdm.json/TeamMembership](https://github.com/microsoft/CDM/tree/master/schemaDocuments/core/applicationCommon/TeamMembership.cdm.json/TeamMembership)

```scala
df.write.format("com.microsoft.cdm")
 .option("storage", "mystorage.dfs.core.windows.net")
 .option("container", "cdmdata")
 .option("manifest", "Teams/root.manifest.cdm.json")
 .option("entity", "TeamMembership")
 .option("entityDefinition", "core/applicationCommon/TeamMembership.cdm.json/Tea
mMembership")
 .option("useCdmGithubModelRoot", true)
 .option("useSubManifest", true)
 .option("appId", appid)
 .option("appKey", appkey)
 .option("tenantId", tenantid)
 .mode(SaveMode.Overwrite)
 .save()
```

#### Explicit Write - using an entity definition stored in ADLS

This code writes the dataframe df to a CDM folder with manifest at
[https://mystorage.dfs.core.windows.net/cdmdata/Contacts/root.manifest.cdm.json](https://mystorage.dfs.core.windows.net/cdmdata/Contacts/root.manifest.cdm.json) with the entity Person. Person data is written as new CSV files (by default) which overwrite existing files in the folder.
The entity definition is retrieved from
[https://mystorage.dfs.core.windows.net/models/cdmmodels/core/Contacts/Person.cdm.json/Person](https://mystorage.dfs.core.windows.net/models/cdmmodels/core/Contacts/Person.cdm.json/Person)

```scala
df.write.format("com.microsoft.cdm")
 .option("storage", "mystorage.dfs.core.windows.net")
 .option("container", "cdmdata")
 .option("manifest", "/contacts/root.manifest.cdm.json")
 .option("entity", "Person")
 .option("entityDefinitionContainer", "cdmmodels")
 .option("entityDefinitionModelRoot", "core")
 .option("entityDefinition", "/Contacts/Person.cdm.json/Person")
 .option("appId", appid)
 .option("appKey", appkey)
 .option("tenantId", tenantid)
 .mode(SaveMode.Overwrite)
 .save()
```

#### Read

This code reads the Person entity from the CDM folder with manifest at [https://mystorage.dfs.core.windows.net/cdmdata/contacts/root.manifest.cdm.json](https://mystorage.dfs.core.windows.net/cdmdata/contacts/root.manifest.cdm.json)

```scala
val df = spark.read.format("com.microsoft.cdm")
 .option("storage", "mystorage.dfs.core.windows.net")
 .option("container", "cdmdata")
 .option("manifest", "/contacts/root.manifest.cdm.json")
 .option("entity", "Person")
 .option("useCDMGithub", true)
 .option("appId", appid)
 .option("appKey", appkey)
 .option("tenantId", tenantid)
 .load()
```

### Other Considerations

#### Handling Date and Time Formats

Date and Time datatype values are handled as normal for Spark and parquet, and in CSV are
read/written in ISO 8601 format.

In CDM, DateTime datatype values are *interpreted as UTC*, and in CSV written in ISO 8601 format, e.g.
2020-03-13 09:49:00Z.

DateTimeOffset values intended for recording local time instants are handled differently in Spark and
parquet from CSV. While CSV and other formats can express a local time instant as a structure,
comprising a datetime and a UTC offset, formatted in CSV like, 2020-03-13 09:49:00-08:00, Parquet and
Spark don’t support such structures. Instead, they use a TIMESTAMP datatype that allows an instant to
be recorded in UTC time (or in some unspecified time zone).

The Spark CDM connector will convert a DateTimeOffset value in CSV to a UTC timestamp. This will be persisted as a Timestamp in parquet and if subsequently persisted to CSV, the value will be serialized as a DateTimeOffset with a +00:00 offset. Importantly, there is no loss of temporal accuracy – the serialized values represent the same instant as the original values, although the offset is lost. Spark systems use their system time as the baseline and normally express time using that local time. UTC
times can always be computed by applying the local system offset. Note that for Azure systems in all regions, system time is always UTC, so all timestamp values will normally be in UTC.

As Azure system values are always UTC, when using implicit write, where a CDM definition is derived from a dataframe, timestamp columns are translated to attributes with CDM DateTime datatype, which implies
a UTC time.

If it is important to persist a local time and the data will be processed in Spark or persisted in parquet,
then it is recommended to use a DateTime attribute and keep the offset in a separate attribute, for
example as a signed integer value representing minutes. In CDM, DateTime values are UTC, so the
offset must be applied when needed to compute local time.

In most cases, persisting local time is not important. Local times are often only required in a UI for user
convenience and based on the user’s time zone, so not storing a UTC time is often a better solution.

#### Time value accuracy

The Spark CDM Connector supports time values with seconds having up to 6 decimal places, based on the format of the data either in the file being read (CSV or Parquet) or as defined in the dataframe, enabling accuracy from single seconds to microseconds.

#### Folder organization

When writing CDM folders, the following folder organization is used. Data files are written into a single data folder. Options for organizing data files into subfolders will be supported in a later release.

Explicit Write (defined by a referenced entity definition)
```
+-- <CDMFolder>
     |-- default.manifest.cdm.json     << with entity ref and partition info
     +-- <Entity>
          |-- <entity>.cdm.json        << resolved physical entity definition
          +-- Data
              +-- data files…              
 ```
Explicit Write with sub-manifest:
```
+-- <CDMFolder>
    |-- default.manifest.cdm.json       << contains reference to sub-manifest
    +-- <Entity>
         |-- <entity>.cdm.json
         |-- <entity>.manifest.cdm.json << sub-manifest with partition info
         +-- Data
             +-- data files...
```
Implicit (entity definition is derived from dataframe schema)
```
+-- <CDMFolder>
    |-- default.manifest.cdm.json
    +-- <Entity>
         |-- <Entity>.cdm.json          << resolved physical entity definition
         +-- Data
         |   +-- data files...
         +-- LogicalDefinition
             +-- <entity>.cdm.json      << logical entity definition(s)
```
Implicit Write with sub-manifest:
```
+-- <CDMFolder>
    |-- default.manifest.cdm.json       << contains reference to sub-manifest
    +-- <Entity>
        |-- <entity>.cdm.json           << resolved physical entity definition
        |-- <entity>.manifest.cdm.json  << sub-manifest with partition info
        +-- Data
        |   +-- data files…
        +-- LogicalDefinition
            +-- <entity>.cdm.json       << logical entity definition(s)
```

## Troubleshooting and Known issues

- When using parquet in Azure Databricks, lzo compression is not currently supported.
- When using implicit write, the implied entity definition is written into a subfolder and has
attributes declared with a CDM data format rather than a CDM data type. These declarations
will be changed in a later release to use CDM primitive data types as normally used in a CDM
logical entity definition.
- Does not yet support general use of alias definitions in import statements; allows the 'cdm' alias to be resolved to the CDM GitHub. 
- If writing Parquet from Synapse, an additional option is required .option("databricks", false)
- Ensure folder and file names are URL encoded if spaces or special characters are used.
- Ensure the decimal precision and scale of decimal data type fields used in the dataframe match the data type used in the CDM entity definition - requires pecision and scale traits are defined on the data type.  If the precision and scale are not defined explicitly in CDM, the default is Decimal(18,4)   

## Not yet supported

The following features are not yet supported:
- Use of headers when reading or writing CSV files and using a separator character other than a comma.
- Use and configuration of partition patterns for organizing data files on write.
- Nested parquet support.
- In explicit write, use of a distinct ADLS storage account for the entity definition. Initially, the
entity definition must be in the same storage account as the target CDM folder.

## Samples

See https://github.com/Azure/spark-cdm-connector/tree/master/samples for sample code and CDM files.

## Changes to this doc
|**Date**  |**Change**|
|------ |---------|
|5/4/20 | Clarified that Overwrite and Append save modes do not allow schema change <br/> Clarified in capabilities summary that partition patterns are supported on read but not write|
|5/6/20 | Clarified that on read, entity files of different format are combined into one dataframe|
|5/11/20|Removed known problem regarding number of rows < executors; fixed in v0.8.|
|5/15/20|Clarified that aliases are not yet supported <br/> Clarified that schema drift and schema evolution are not supported|
|6/1/20| Noted that an additional option is required when writing Parquet from Synapse to _Known issues_<br/>Added reference to using API to get the current library version|
|6/23/20| Noted that folder and file names must be URL encoded, decimal precision and scale must match CDM datatypes used.|
