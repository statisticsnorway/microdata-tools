# THE METADATA MODEL
_______
In addition to the examples of metadata json files present in this repository, this document briefly describes the fields in the metadata model.

## Root level fields
These fields describe the dataset as a whole.

**temporalityType** <span class ="mdata-red-text">(required)</span>: The temporality type of the dataset. Must be *one* of:  

```json
"temporalityType": "FIXED" | "ACCUMULATED" | "STATUS" | "EVENT",
```
**sensitivityLevel** <span class ="mdata-red-text">(required)</span>: The sensitivity of the data in the dataset. Must be *one* of:

```json
"sensitivityLevel": "PERSON_GENERAL" | "PERSON_SPECIAL" | "PUBLIC" | "NONPUBLIC",
```

* PERSON_GENERAL: General personal data, this category applies to information that is generally handled without further notification and is not especially sensitive. Email address is an example.
* PERSON_SPECIAL: Special category of personal data, this is a category of data that is more sensitive. Health information is an example.
* PUBLIC: Data that is publicly available
* NONPUBLIC: Data that is not publicly available

**populationDescription**<span class ="mdata-red-text"> (required)</span>: Description of the dataset's population.
```json
"populationDescription": [
    {
        "languageCode": "no",
        "value": "Alle personer registrert bosatt i Norge"
    }
],
```

**spatialCoverageDescription**<span class ="mdata-blue-2-text"> (optional)</span>: The geographic area relevant to the data.
```json
"spatialCoverageDescription": [{"languageCode": "no", "value": "Norge"}],
```

**subjectFields**<span class ="mdata-red-text"> (required)</span>: Tag(s).
```json
"subjectFields": [
	[{"languageCode": "no", "value": "BEFOLKNING"}],
    [{"languageCode": "no", "value": "SAMFUNN"}]
],
```


## Datarevision
These fields describe the current version of the dataset.

* **description** <span class ="mdata-red-text">(required)</span>: Description of this version of the dataset.  
* **temporalEnd**<span class ="mdata-blue-2-text"> (optional)</span>: Description of why this dataset will not be updated anymore. Successor datasets can be optionally specified. 

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
        "successors": "Navn på erstatter",
    }
},
```

## Identifier variables
Description of the indentifier column of the dataset. It is represented as a list in the metadata model, but currently only one identifier is allowed per dataset. The identifiers are always based on a unit. A unit is centrally defined to make joining datasets across datastores easy.

* **unitType**<span class ="mdata-red-text"> (required)</span>: The unitType for this dataset identifier column. Must be *one* of: 

```json
"identifierVariables": [
    {
        "unitType": "FAMILIE" | "FORETAK" | "HUSHOLDNING" | "JOBB" |
                    "KJORETOY" | "KOMMUNE" | "KURS" | "PERSON" | "VIRKSOMHET"
    }
],
```


## Measure variables
Description of the measure column of the dataset. It is represented as a list in the metadata model, but currently only one measure is allowed per dataset.

If the measure column in your dataset is in fact a unit type (PERSON (FNR), VIRKSOMHET (ORGNR) etc.), the metadatamodel for the measure variable is different than described below. You should skip to the section [Measure variables (with unitType)](#measure-variables-with-unittype). 


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
Describes the Value domain for the relevant variable. Either by codeList (enumerated value domain), or a description of expected values (described value domain).

* **description**<span class ="mdata-red-text"> (required in described value domain)</span>: A description of the domain. Example for the variable "BRUTTO_INNTEKT": "Alle positive tall".
* **measurementUnitDescription**<span class ="mdata-red-text"> (required in described value domain)</span>: A description of the unit measured. Example: "Norske Kroner"
* **measurementType**<span class ="mdata-red-text"> (required in described value domain)</span>: A machine readable definisjon of the unit measured. One of: [CURRENCY, WEIGHT, LENGTH, HEIGHT, GEOGRAPHICAL]
* **uriDefinition** <span class ="mdata-blue-2-text"> (optional)</span>: Link to external resource describing the domain.
* **codeList**<span class ="mdata-red-text"> (required in enumerated value domain)</span>: A code list of valid codes for the domain, description, and their validity period. The metadata fields for each item in the codelist are: 
    * code <span class ="mdata-red-text"> (required)</span>: The code itself. Example: "0301"
    * categoryTitle <span class ="mdata-red-text"> (required)</span>: The category name of the code. Example: "Oslo"
    * validFrom <span class ="mdata-red-text"> (required)</span>: The code is valid from date YYYY-MM-DD  
    * validUntil <span class ="mdata-blue-2-text"> (optional)</span>: The code is valid until date YYYY-MM-DD
* **sentinelAndMissingValues**<span class ="mdata-blue-2-text"> (optional in enumerted value domain)</span>: A code list where the codes represent missing or sentinel values that, while not entirely valid, are still expected to appear in the dataset.
    * code <span class ="mdata-red-text"> (required)</span>: The code itself. Example: 0
    * categoryTitle <span class ="mdata-red-text"> (required)</span>: The category name of the code. Example: "Unknown value"



Here is an example of two different value domains.
The first value domain belongs to a measure for dataset where the measure is a persons accumulated gross income:
```json
"valueDomain": {
    "uriDefinition": [],
    "description": [{"languageCode": "no", "value": "Norske Kroner"}],
    "measurementType": "CURRENCY",
    "measurementUnitDescription": [{"languageCode": "no", "value": "Norske Kroner"}],
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
* **VIRKSOMHET**: Representation of a virksomhet in the microdata.no platform. Columns with this unit type should contain ORGNR.
* **HUSHOLDNING**: Representation of a husholdning in the microdata.no platform. Columns with this unit type should contain FNR.
* **JOBB**: Representation of a job in the microdata.no platform. Columns with this unit type should contain FNR_ORGNR. FNR belongs to the employee and ORGNR belongs to the employer.
* **KOMMUNE**: Representation of a kommune in the microdata.no platform. Columns with this unit type should contain a valid kommune number.
* **KURS**: Representation of a course in the microdata.no platform. Columns with this unit type should contain FNR_KURSID. Where FNR belongs to the participant and KURSID is the NUDB course id.
* **KJORETOY**: Representation of a vehicle in the microdata.no platform. Columns with this unit type should contain FNR_REGNR. Where FNR is the owner of the vehicle, and REGNR is the registration number for the vehicle.

## Validation

### Creating a datafile
A data file must be supplied as a csv file with semicolon as the column seperator. There must always be 5 columns present in this order:
1. identifier
2. measure
3. start
4. stop
5. empty column (This column is reserved for an extra attribute variable if that is considered necessary. Example: Datasource)

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
    - All rows must have an unique identifier. (No repeating identifiers within a dataset)
    - All rows must have a stop date
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