# magen-cpp

#### Consumer API 
 See [DLP Masking API](https://github.ibm.com/data-privacy/magen-cpp/blob/main/include/DLPMasking.h)
 
#### Example usage
 See [this UDF](https://github.ibm.com/data-privacy/dbengines-adp/blob/dev/teradata/src/mask.cpp)
 
#### Consumer Requirements
1 - copy the 'resources' folder of this repository to HOME folder on the DB server  
2 - Inside the Database UDF code, set the env variable DP_DICT_RESOURCES to this folder.
```
 // for e.g. for Db2
 setenv("DP_DICT_RESOURCES", (string(getenv("DB2_HOME")) + string("/resources")).c_str(), 0);
 
```

#### Install Pre-requisistes

```
sh prereqs-<os>.sh
```

#### Build

Generate build configuration files for your platform. 
Run make and tests.
```
sh cmake-<os>.sh
```

#### Types of Masking supported currently
 - Full Redact & Partial Redact
 - Substitution
 - Full Obfuscation & Partial Obfuscation
 - Identifier method
 - Date Shifting
 - Date Preserve Period

#### Formats Currently supported for FormatPreserving Obfuscation 
 - AirportCodes
 - AlabamaStateDriversLicense
 - AlaskaStateDriversLicense
 - AlbertaProvinceDriversLicense
 - AmericanExpressCard
 - ArizonaStateDriversLicense
 - ArkansasStateDriversLicense
 - BIC
 - BritishColumbiaProvinceDriversLicense
 - CaliforniaStateDriversLicense
 - CageCode
 - CanadianSIN
 - CaPostalCode
 - CAProvinceCodes
 - CAProvinceNames
 - CitiesEnglish
 - ColoradoStateDriversLicense
 - ConnecticutStateDriversLicense
 - Coordinate
 - CountryCodes_CaseSensitive
 - Countries_English
 - CPTCodes
 - CreditCard
 - Date
 - Datetime
 - DelawareStateDriversLicense
 - DiscoverCard
 - DinersClubCard
 - DinersClubInternationalCard
 - DinersClubUSACanadaCard
 - DUNS
 - EmploymentStatus
 - EnglishEmailCaseSensitive
 - EnglishNIN
 - Ethnicity
 - EyeColors
 - FloridaStateDriversLicense
 - Fortune1000
 - FrenchINSEE
 - Gender
 - GeorgiaStateDriversLicense
 - GermanVehicleRegistration
 - HairColors
 - HawaiiStateDriversLicense
 - HealthInsuranceClaimNumber
 - Hobbies
 - Honorifics
 - Hostname
 - IBAN
 - ICD10Codes
 - IdahoStateDriversLicense
 - IllinoisStateDriversLicense
 - IMEI
 - IndianaStateDriversLicense
 - IowaStateDriversLicense
 - IP
 - IPV6Address
 - INCOTerms
 - IrelandEircode
 - ISBN
 - ISICCodes
 - ISIN
 - ISOStateProvinceCode
 - ItalianFiscalCode
 - JapanCreditBureauCard
 - KansasStateDriversLicense
 - KentuckyStateDriversLicense
 - LanguageList
 - Latitude
 - Longitude
 - LouisianaStateDriversLicense
 - MacAddress
 - MaineStateDriversLicense
 - ManitobaProvinceDriversLicense
 - MaritalStatus
 - MarylandStateDriversLicense
 - MassachusettsStateDriversLicense
 - MasterCard
 - MichiganStateDriversLicense
 - MinnesotaStateDriversLicense
 - MississippiStateDriversLicense
 - MissouriStateDriversLicense
 - MontanaStateDriversLicense
 - Month
 - NameSuffix
 - NebraskaStateDriversLicense
 - NevadaStateDriversLicense
 - NewBrunswickProvinceDriversLicense
 - NewfoundlandandLabradorProvinceDriversLicense
 - NewHampshireDriversLicense
 - NewJerseyStateDriversLicense
 - NewMexicoStateDriversLicense
 - NewYorkStateDriversLicense
 - NorthAmericaPhone
 - NorthCarolinaStateDriversLicense
 - NorthDakotaStateDriversLicense
 - NovaScotiaProvinceDriversLicense
 - OhioStateDriversLicense
 - OklahomaStateDriversLicense
 - OntarioProvinceDriversLicense
 - OregonStateDriversLicense
 - Organization
 - PennsylvaniaStateDriversLicense
 - Percent
 - PoliticalParties
 - PrinceEdwardIslandProvinceDriversLicense
 - QuebecProvinceDriversLicense
 - Relationship
 - Religions
 - RhodeIslandStateDriversLicense
 - SaskatchewanProvinceDriversLicense
 - SouthCarolinaStateDriversLicense
 - SouthDakotaStateDriversLicense
 - SpanishNIF
 - SSN4
 - StateProvinceName
 - TennesseeStateDriversLicense
 - TexasStateDriversLicense
 - UKPostCode
 - UKProvinceCodes
 - URL
 - UniversalProductCode
 - USCounty
 - USSICCode
 - USStateCodes
 - USStateNames
 - USAddressSimple
 - UsaSocialSecurityNumber
 - USFirstName
 - USLastName
 - USNationalDrugCode
 - USPersonName
 - USRoutingTransitNumber
 - USSICCode
 - USStateCapitals
 - USStreetNames 
 - USZipCodes
 - UtahStateDriversLicense
 - VehicleIdentificationNumber
 - VermontStateDriversLicense
 - VirginiaStateDriversLicense
 - VisaCard
 - WashingtonStateDriversLicense
 - WashingtonDCStateDriversLicense
 - WestVirginiaStateDriversLicense
 - WisconsinStateDriversLicense
 - WyomingStateDriversLicense

 ### IBAN Formats supported:

- AlbaniaIban
- AndorraIban
- ArabEmiratesIban
- AustriaIban
- AzerbaijanIban
- BahrainIban
- BelarusIban
- BelgiumIban
- BosniaAndHerzegovina
- BrazilIban
- BulgariaIban
- CostaRicaIban
- CroatiaIban
- CyprusIban
- CzechRepublicIban
- DenmarkIban
- DominicanRepublicIban
- EastTimorIban
- EgyptIban
- EstoniaIban
- FaroeIslandsIban
- FinlandIban
- FranceIban
- GeorgiaIban
- GermanIban
- GibraltarIban
- GreatBritainIban
- GreeceIban
- GreenlandIban
- GuatamalaIban
- HungaryIban
- IcelandIban
- IraqIban
- IrelandIban
- IsraelIban
- ItalyIban
- JordanIban
- KazakhtanIban
- KosovoIban
- KuwaitIban
- LatviaIban
- LebanonIban
- LiechtensteinIban
- LithuaniaIban
- LuxemburgIban
- MacedoniaIban
- MaltaIban
- MauritaniaIban
- MauritiusIban
- MoldovaIban
- MonacoIban
- MontenegroIban
- NetherlandsIban
- NorwayIban
- PakistanIban
- PalestineIban
- PolandIban
- PortugalIban
- QatarIban
- RomaniaIban
- SaintLuciaIban
- SanMarinoIban
- SaoTomeIban
- SaudiArabiaIban
- SerbiaIban
- SeychellesIban
- SlovakiaIban
- SloveniaIban
- SpainIban
- SwedenIban
- SwitzerlandIban
- TunisiaIban
- TurkeyIban
- UkraineIban
- VaticanIban
- VirginIslandsIban

#### All Formats that will be supported for FormatPreserving Obfuscation.
 [All these will be supported without returning error.](https://github.ibm.com/data-privacy/lts-masking/blob/main/src/input/formats/Formats.cpp#L8)
 Any in the this list and not yet in currently supported list, will fall back to Substitution masking.
