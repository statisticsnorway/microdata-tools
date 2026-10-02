# THE METADATA MODEL
_______
This document is a field-by-field reference for the metadata JSON file that accompanies each dataset. It describes what each field means and how to fill it in, so you can write a valid metadata file for your dataset. For a step-by-step walkthrough of preparing, validating and packaging a full dataset, see [Getting started](index.md). For complete, valid metadata files, see [the example datasets](https://github.com/statisticsnorway/microdata-tools/tree/main/docs/examples).


## Root level fields
These fields describe the dataset as a whole.

**temporalityType** <span class ="mdata-red-text">(required)</span>: The temporality type of the dataset. Must be *one* of:  

```json
"temporalityType": "FIXED" | "STATUS" | "ACCUMULATED" | "EVENT",
```

- `FIXED`: A value that does not change over time for a given unit, e.g. place of birth.
- `STATUS`: A value observed at specific points in time, e.g. employment status at a given date.
- `ACCUMULATED`: A value accumulated over a period, e.g. yearly income.
- `EVENT`: A state that is valid over a period of time. A unit can have only one state at a given point in time, but the state may change over time, e.g. marital status or place of residence.

**sensitivityLevel** <span class ="mdata-red-text">(required)</span>: The sensitivity of the data in the dataset. Must be *one* of:

```json
"sensitivityLevel": "PERSON_GENERAL" | "PERSON_SPECIAL" | "PUBLIC" | "NONPUBLIC",
```

* `PERSON_GENERAL`: General personal data, this category applies to information that is generally handled without further notification and is not especially sensitive. Email address is an example.
* `PERSON_SPECIAL`: Special category of personal data, this is a category of data that is more sensitive. Health information is an example.
* `PUBLIC`: Data that is publicly available
* `NONPUBLIC`: Data that is not publicly available

**populationDescription**<span class ="mdata-red-text"> (required)</span>: Description of the population of statistical units this variable's values apply to.

The population is not necessarily a population of persons, it refers to whatever set of units is represented by this variable. The unit type is identified in [identifierVariables](#identifier-variables). Depending on the dataset this can be persons, enterprises, families, vehicles, accidents and so on.

Examples for a dataset where the statistical unit is PERSON:

```json
"populationDescription": [
    {
        "languageCode": "no",
        "value": "Hovedsakelig bosatte i Norge (fnr), men noen ikke-bosatte kan inngå (dnr)"
    }
],
```
```json
"populationDescription": [
    {
        "languageCode": "no",
        "value": "Variabelen omfatter personer registrert med en fullført utdanning på hovedfags-/masternivå."
    }
],
```

Example for a dataset where the statistical unit is TRAFIKKULYKKE (traffic accident):

```json
"populationDescription": [
    {
        "languageCode": "no",
        "value": "Variabelen omfatter dødsulykker og andre ulykker med personskade som er meldt til politiet."
    }
],
```

**spatialCoverageDescription**<span class ="mdata-blue-2-text"> (optional)</span>: The geographic area covered by the population described in `populationDescription`. For datasets with national coverage, the value should simply be "Norge":
```json
"spatialCoverageDescription": [{"languageCode": "no", "value": "Norge"}],
```

**subjectFields**<span class ="mdata-red-text"> (required)</span>: Tag(s).
```json
"subjectFields": [
	[{"languageCode": "no", "value": "Befolkning"}],
    [{"languageCode": "no", "value": "Husholdning"}],
    [{"languageCode": "no", "value": "Barn, Familie og husholdninger"}]
],
```


## Datarevision
These fields describe the current publication of the dataset — that is, what's new or changed about *this variable*, not the databank-version in microdata.no (i.e version 54).

**description** <span class ="mdata-red-text">(required)</span>: A short description of what's new in this publication of the variable. For a variable's first publication, "Første publisering" can be used. For later publications, you can briefly describe what has been updated, added or corrected, for example "Oppdaterte tall for 2026".

**temporalEnd**<span class ="mdata-blue-2-text"> (optional)</span>: Use this field when the dataset has been discontinued and will no longer receive updates.
    - `description`: Explain why the dataset is no longer updated.
    - `successors`: Name the dataset(s) that replaces this dataset, if applicable.

```json
"dataRevision": {
    "description": [{"languageCode": "no", "value": "Nye årganger."}],
    "temporalEnd": {
        "description": [
            {
                "languageCode": "no",
                "value": "Videre oppdateringer utgår pga..."
            }
        ],
        "successors": ["Navn på erstatter"]
    }
},
```

## Identifier variables
Description of the indentifier column of the dataset. It is represented as a list in the metadata model, but currently only one identifier is allowed per dataset. The identifiers are always based on a unit. A unit is centrally defined to make joining datasets across datastores easy.

* **unitType**<span class ="mdata-red-text"> (required)</span>: The unitType for this dataset identifier column. Must be one of the predefined unit types in microdata-tools. [See complete list of unit types here](https://github.com/statisticsnorway/microdata-tools/tree/main/microdata_tools/validation/components/unit_type_variables). Examples of unit types are:
 `PERSON`, `HUSHOLDNING`, `BEDRIFT`, `JOBB`, `KOMMUNE`.
 See more unit type examples and descriptions under the [Unit types](#unit-types) section.

```json
"identifierVariables": [
    {
        "unitType": "PERSON"
    }
],
```


## Measure variables
Description of the measure column of the dataset. It is represented as a list in the metadata model, but currently only one measure is allowed per dataset.

If the measure column in your dataset is in fact a unit type (PERSON (FNR), BEDRIFT (ORGNR) etc.), the metadatamodel for the measure variable is different than described below. You should skip to the section [Measure variables (with unitType)](#measure-variables-with-unittype). 


* **name**<span class ="mdata-red-text"> (required)</span>: Human readable name(Label) of the measure column. This should be similar to your dataset name. Example for PERSON_INNTEKT.json: "Person inntekt".
* **description**<span class ="mdata-red-text"> (required)</span>: Description of the column contents. Example: "Skattepliktig og skattefritt utbytte i... "
* **dataType** <span class ="mdata-red-text"> (required)</span>: DataType for the values in the column. One of: ["STRING", "LONG", "DOUBLE", "DATE"]
* **format** <span class ="mdata-blue-2-text"> (optional)</span>: More detailed description of the values. For example if dataType for the measure is DATE, you can specify YYYYMM, YYYYMMDD etc.
* **uriDefinition** <span class ="mdata-blue-2-text"> (optional)</span>: Link to external resource describing the measure.
* **valueDomain**<span class ="mdata-red-text"> (required)</span>: See definition below.

```json
"measureVariables": [
    {
        "name": [{"languageCode": "no", "value": "Person inntekt"}],
        "description": [
            {
                "languageCode": "no",
                "value": "Personens rapporterte inntekt"
            }
        ],
        "dataType": "DOUBLE",
        "valueDomain": {...}
    }
]
```
### Value domain
A value domain describes which values are valid for the measure. There are two types of value domains:  
**Enumerated value domain**: Used when the values are a defined set of codes, for example `1 = Mann`, `2 = Kvinne`.  
**Described value domain**: Used when the values are described, e.g. by a measurement unit, for example an amount in NOK.

* **description**<span class ="mdata-red-text"> (required in described value domain)</span>: A description of the domain. Example for the variable "BRUTTO_INNTEKT": "Beløp".
* **measurementUnitDescription**<span class ="mdata-red-text"> (required in described value domain)</span>: A description of the unit measured. Example: "NOK"
* **measurementType**<span class ="mdata-red-text"> (required in described value domain)</span>: A machine readable definition of the unit measured. E.g. `CURRENCY`, `WEIGHT`, `LENGTH`, `HEIGHT`, `GEOGRAPHICAL`.
* **uriDefinition** <span class ="mdata-blue-2-text"> (optional)</span>: Link to external resource describing the domain.
* **codeList**<span class ="mdata-red-text"> (required in enumerated value domain)</span>: A code list of valid codes for the domain, description, and their validity period. The metadata fields for each item in the codelist are: 
    * code <span class ="mdata-red-text"> (required)</span>: The code itself. Example: "0301"
    * categoryTitle <span class ="mdata-red-text"> (required)</span>: The category name of the code. Example: "Oslo"
    * validFrom <span class ="mdata-red-text"> (required)</span>: The code is valid from date YYYY-MM-DD  
    * validUntil <span class ="mdata-blue-2-text"> (optional)</span>: The code is valid until date YYYY-MM-DD
* **sentinelAndMissingValues**<span class ="mdata-blue-2-text"> (optional in enumerted value domain)</span>: A code list where the codes represent missing or sentinel values that, while not entirely valid, are still expected to appear in the dataset.
    * code <span class ="mdata-red-text"> (required)</span>: The code itself. Example: 0
    * categoryTitle <span class ="mdata-red-text"> (required)</span>: The category name of the code. Example: "Unknown value"



Here is an example of the two different value domains.
The first value domain belongs to a measure for dataset where the measure is a persons accumulated gross income:
```json
"valueDomain": {
    "uriDefinition": [],
    "description": [{"languageCode": "no", "value": "Beløp"}],
    "measurementType": "CURRENCY",
    "measurementUnitDescription": [{"languageCode": "no", "value": "NOK"}],
}
```
This example is what we would call a __described value domain__.

The second example belongs to the measure variable of a dataset where the measure describes the sex of a population:
```json
"valueDomain": {
    "uriDefinition": [],
    "codeList": [
        {
            "code": "1",
            "categoryTitle": [{"languageCode": "no", "value": "Mann"}],
            "validFrom": "1900-01-01"
        },
        {
            "code": "2",
            "categoryTitle": [{"languageCode": "no", "value": "Kvinne"}],
            "validFrom": "1900-01-01"
        }
    ],
    "sentinelAndMissingValues": [
        {
            "code": "0",
            "categoryTitle": [{"languageCode": "no", "value": "Ukjent"}]
        }
    ]
}
```
We expect all values in this dataset to be either "1" or "2", as this dataset only considers "Male" or "Female". But we also expect a code "0" to be present in the dataset, where it represents "Unknown". A row with "0" as measure is therefore not considered invalid. A value domain with a code list like this is what we would call an __enumerated value domain__.

### Measure variables (with unitType)
You might find that some of your datasets contain a unitType in the measure column as well. Let's say you have a dataset PERSON_MOR where the identifier column is a population of unitType "PERSON", and the measure column is a population of unitType "PERSON". The measure here is representing the populations mothers. Then you may define it as such:

* **unitType** <span class ="mdata-red-text"> (required)</span>: The unitType for this dataset measure column. Must be one of the [predefined unit types](#unit-types).
* **name**<span class ="mdata-red-text"> (required)</span>: Human readable name(Label) of the measure column. This should be similar to your dataset name. Example for PERSON_MOR.json: "Person mor".
* **description**<span class ="mdata-red-text"> (required)</span>: Description of the column contents. Example: "Personens registrerte biologiske mor..."

```json
"measureVariables": [
    {
        "unitType": "PERSON",
        "name": [{"languageCode": "no", "value": "Person mor"}],
        "description": [
            {"languageCode": "no", "value": "Personens registrerte biologiske mor"}
        ]
    }
]
```

## Unit types
* **PERSON**: Representation of a person in the microdata.no platform. Columns with this unit type should contain FNR.
* **FAMILIE**: Representation of a family in the microdata.no platform. Columns with this unit type should contain FNR.
* **FORETAK**: Representation of a foretak in the microdata.no platform. Columns with this unit type should contain ORGNR.
* **BEDRIFT**: Representation of a bedrift in the microdata.no platform. Columns with this unit type should contain ORGNR.
* **HUSHOLDNING**: Representation of a husholdning in the microdata.no platform. Columns with this unit type should contain FNR.
* **JOBB**: Representation of a job in the microdata.no platform. Columns with this unit type should contain FNR_ORGNR. FNR belongs to the employee and ORGNR belongs to the employer.
* **KOMMUNE**: Representation of a kommune in the microdata.no platform. Columns with this unit type should contain a valid kommune number.
* **KURS**: Representation of a course in the microdata.no platform. Columns with this unit type should contain FNR_KURSID. Where FNR belongs to the participant and KURSID is the NUDB course id.
* **KJORETOY**: Representation of a vehicle in the microdata.no platform. Columns with this unit type should contain FNR_REGNR. Where FNR is the owner of the vehicle, and REGNR is the registration number for the vehicle.

[See complete list of predefined unit types here](https://github.com/statisticsnorway/microdata-tools/tree/main/microdata_tools/validation/components/unit_type_variables).

## Validation

### Creating a datafile
A data file must be supplied as a csv file with semicolon as the column seperator. There must always be 5 columns present in this order:
1. identifier
2. measure
3. start
4. stop
5. empty column (This column is reserved for an extra attribute variable if that is considered necessary. Example: Datasource)

For a dataset with temporalityType FIXED, the value does not change over time, so there is no time period to describe — both the start and stop columns should be left empty. On microdata.no, this is reflected by showing the variable's validity period as infinite ("∞"). See [Validation rules by temporality type](#validation-rules-by-temporality-type) below for details on stop-column backwards compatibility.

Example:
```
12345678910;1;;;
12345678911;2;;;
12345678912;1;;;
```

This dataset describes the sex of a group of persons. The columns can be described like this:
* Identifier: FNR
* Measure: Sex
* Start: empty
* Stop: empty

Example:
```
12345678910;100000;2020-01-01;2020-12-31;
12345678910;200000;2021-01-01;2021-12-31;
12345678911;100000;2018-01-01;2018-12-31;
12345678911;150000;2020-01-01;2020-12-31;
```

This dataset describes a group of persons gross income accumulated yearly. The columns can be described like this:
* Identifier: FNR
* Measure: Accumulated gross income for the time period
* Start: start of time period
* Stop: end of time period
* Empty column (This column is reserved for an extra attribute variable if that is considered necessary. As there is no need here, it remains empty.)

### General validation rules for data
* There can be no empty rows in the dataset
* There can be no more than 5 elements in a row
* Every row must have a non-empty identifier
* Every row must have a non-empty measure
* Values in the stop- and start-columns must be formatted correctly: "YYYY-MM-DD". Example "2020-12-31".
* The data file must be utf-8 encoded

### Validation rules by temporality type
* **FIXED** (Constant value, ex.: place of birth)
    - All rows must have a unique identifier (no repeating identifiers within a dataset)
    - The start-column must be empty. A non-empty start date is a validation error.
    - The stop-column is ignored and should be left empty. Non-empty stop dates are accepted for backwards compatibility and produce a deprecation warning, but the values are discarded and never used. Support for non-empty stop dates will be removed in a future version.
* **STATUS** (measurement taken at a certain point in time. (cross section))
    - All rows must have a start date
    - All rows must have a stop date
    - Start and stop date must be equal for any given row
* **ACCUMULATED** (Accumulated over a period. Ex.: yearly income)
    - All rows must have a start date
    - All rows must have a stop date
    - Start can not be later than stop
    - Time periods for the same identifiers must not intersect
* **EVENT** (data state for validity period)
    - All rows must have a start date
    - If there is a non-empty value in the stop column for a given row; start can not be later than stop
    - Time periods for the same identifiers must not intersect (A row without a stop date is considered an ongoing event, and will intersect with all timespans after its start date)
