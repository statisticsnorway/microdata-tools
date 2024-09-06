# THE METADATA MODEL
_______
In addition to the examples of metadata json files present in this repository, this document briefly describes the fields in the metadata model.

### ROOT LEVEL FIELDS
These fields describe the dataset as a whole.
* **temporalityType**: The temporality type of the dataset. Must be one of FIXED, ACCUMULATED, STATUS or EVENT.
* **sensitivityLevel**: The sensitivity of the data in the dataset. Must be one of: PERSON_GENERAL, PERSON_SPECIAL, PUBLIC or NONPUBLIC.
  * PERSON_GENERAL: general personal data, this category applies to information that is generally handled without further notification and is not especially sensitive. Email address is an example.
  * PERSON_SPECIAL: special category of personal data, this is a category of data that is more sensitive. Health information is an example.
  * PUBLIC: data that is publicly available
  * NONPUBLIC: data that is not publicly available
* **spatialCoverageDescription**: The geographic area relevant to the data.
* **populationDescription**: Description of the dataset's population.


### DATAREVISION
These fields describe the current version of the dataset.
* **description**: Description of this version of the dataset.
* **temporalEnd (Optional)**: Description of why this dataset will not be updated anymore. Successor datasets can be optionally specified. 

### IDENTIFIER VARIABLES
Description of the indentifier column of the dataset. It is represented as a list in the metadata model, but currently only one identifier is allowed per dataset. The identifiers are always based on a unit. A unit is centrally defined to make joining datasets across datastores easy.
* **unitType**: The unitType for this dataset identifier column. Must be one of: FAMILIE, FORETAK, HUSHOLDNING, JOBB, KJORETOY, KOMMUNE, KURS, PERSON or VIRKSOMHET.

### MEASURE VARIABLES
Description of the measure column of the dataset. It is represented as a list in the metadata model, but currently only one measure is allowed per dataset.
* **name**: Human readable name(Label) of the measure column. This should be similar to your dataset name. Example for PERSON_INNTEKT.json: "Person inntekt".
* **description**: Description of the column contents. Example: "Skattepliktig og skattefritt utbytte i... "
* **dataType**: DataType for the values in the column. One of: ["STRING", "LONG", "DOUBLE", "DATE"]
* **format (Optional)**: More detailed description of the values. For example a regular expression.
* **uriDefinition (Optional)**: Link to external resource describing the measure.
* **valueDomain**: See definition below.


### MEASURE VARIABLES (with unitType)
You might find that some of your datasets contain a unitType in the measure column as well. Let's say you have a dataset PERSON_MOR where the identifier column is a population of unitType "PERSON", and the measure column is a population of unitType "PERSON". The measure here is representing the populations mothers. Then you may define it as such:
* **unitType**: The unitType for this dataset measure column. Must be one of: FAMILIE, FORETAK, HUSHOLDNING, JOBB, KJORETOY, KOMMUNE, KURS, PERSON or VIRKSOMHET.
* **name**: Human readable name(Label) of the measure column. This should be similar to your dataset name. Example for PERSON_MOR.json: "Person mor".
* **description**: Description of the column contents. Example: "Personens registrerte biologiske mor... "


### VALUE DOMAIN
Describes the Value domain for the relevant variable. Either by codeList(enumerated value domain), or a description of expected values(described value domain).
* **description**: A description of the domain. Example for the variable "BRUTTO_INNTEKT": "Alle positive tall".
* **measurementUnitDescription**: A description of the unit measured. Example: "Norske Kroner"
* **measurementType**: A machine readable definisjon of the unit measured. One of: [CURRENCY, WEIGHT, LENGTH, HEIGHT, GEOGRAPHICAL]
* **uriDefinition**: Link to external resource describing the domain.
* **codeList**: A code list of valid codes for the domain, description, and their validity period.
* **sentinelAndMissingValues**: A code list where the codes represent missing or sentinel values that, while not entirely valid, are still expected to appear in the dataset. Example: Code 0 for "Unknown value".


Here is an example of two different value domains.
The first value domain belongs to a measure for dataset where the measure is a persons accumulated gross income:
```json
"valueDomain": {
    "uriDefinition": [],
    "description": [{"languageCode": "no", "value": "Norske Kroner"}],
    "measurementType": "CURRENCY",
    "measurementUnitDescription": [{"languageCode": "no", "value": "Norske Kroner"}]
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
            "categoryTitle": [{"languageCode": "no", "value": "Ukjent"}],
            "validFrom": "1900-01-01"
        }
    ]
}
```
We expect all values in this dataset to be either "1" or "2", as this dataset only considers "Male" or "Female". But we also expect a code "0" to be present in the dataset, where it represents "Unknown". A row with "0" as measure is therefore not considered invalid. A value domain with a code list like this is what we would call an __enumerated value domain.


### UNIT TYPES
* **PERSON**: Representation of a person in the microdata.no platform. Columns with this unit type should contain FNR.
* **FAMILIE**: Representation of a family in the microdata.no platform. Columns with this unit type should contain FNR.
* **FORETAK**: Representation of a foretak in the microdata.no platform. Columns with this unit type should contain ORGNR.
* **VIRKSOMHET**: Representation of a virksomhet in the microdata.no platform. Columns with this unit type should contain ORGNR.
* **HUSHOLDNING**: Representation of a husholdning in the microdata.no platform. Columns with this unit type should contain FNR.
* **JOBB**: Representation of a job in the microdata.no platform. Columns with this unit type should contain FNR_ORGNR. FNR belongs to the employee and ORGNR belongs to the employer.
* **KOMMUNE**: Representation of a kommune in the microdata.no platform. Columns with this unit type should contain a valid kommune number.
* **KURS**: Representation of a course in the microdata.no platform. Columns with this unit type should contain FNR_KURSID. Where FNR belongs to the participant and KURSID is the NUDB course id.
* **KJORETOY**: Representation of a vehicle in the microdata.no platform. Columns with this unit type should contain FNR_REGNR. Where FNR is the owner of the vehicle, and REGNR is the registration number for the vehicle.

## VALIDATION

### CREATING A DATAFILE
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

### GENERAL VALIDATION RULES FOR DATA
* There can be no empty rows in the dataset
* There can be no more than 5 elements in a row
* Every row must have a non-empty identifier
* Every row must have a non-empty measure
* Values in the stop- and start-columns must be formatted correctly: "YYYY-MM-DD". Example "2020-12-31".
* The data file must be utf-8 encoded

### VALIDATION RULES BY TEMPORALITY TYPE
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