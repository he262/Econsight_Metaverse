Feature: Test for EconsightMetaverse
    Background: 
    Given Fetch the EconsightMetaverse data from Features\\Data\\EconsightMetaverse.sql
    Given Fetch the EconsightMetaverse from api http://brutus2.bat.ci.dom/sidwebapi/api/Index/GetUniverse

    Scenario Outline: Validate the EconsightMetaverse with Database
    When Read the Following parameters
        | Param_name   | Param_value                                                        |
        | indexSymbol  | <IndexSymbol>                                                      |
        | compDate     | <Cutoff_date>                                                      |
        | secAttrDate  | <Cutoff_date>                                                      |
        | calendarName | STOXXCAL                                                           |
        | vendorItems  | EconsightMetaverse,EconsightLithiumBatteries,EconsightEnergyPatent |
        | output       | csv                                                                |

    Then  Make api requests
    Then save the api reponse
    When Fetch the Data for EconsightMetaverse from DB at <Cutoff_date>
    Then validate the data


    Examples:
        | IndexSymbol | Cutoff_date |
        | TW1P        | 2024-05-31  |
        | TW1P        | 2024-04-30  |
        | TW1P        | 2024-03-28  |
        | TW1P        | 2024-02-29  |
        | TW1P        | 2024-01-31  |
        | TW1P        | 2023-12-29  |
        | TW1P        | 2023-11-30  |
        | TW1P        | 2023-10-31  |
        | TW1P        | 2023-09-29  |
        | TW1P        | 2023-08-31  |
        | TW1P        | 2023-07-31  |
        | TW1P        | 2023-06-30  |
        | TW1P        | 2023-05-31  |
        | TW1P        | 2023-04-28  |
        | TW1P        | 2023-03-31  |
        | TW1P        | 2023-02-28  |
        | TW1P        | 2023-01-31  |
        | TW1P        | 2022-12-30  |
        | TW1P        | 2022-11-30  |
        | TW1P        | 2022-10-31  |
        | TW1P        | 2022-09-30  |
        | TW1P        | 2022-08-31  |
        | TW1P        | 2022-07-29  |
        | TW1P        | 2022-06-30  |
        | TW1P        | 2022-05-31  |
        | TW1P        | 2022-04-29  |
        | TW1P        | 2022-03-31  |
        | TW1P        | 2022-02-28  |
        | TW1P        | 2022-01-31  |
        | TW1P        | 2021-12-31  |
        | TW1P        | 2021-11-30  |
        | TW1P        | 2021-10-29  |
        | TW1P        | 2021-09-30  |
        | TW1P        | 2021-08-31  |
        | TW1P        | 2021-07-30  |
        | TW1P        | 2021-06-30  |
        | TW1P        | 2021-05-31  |
        | TW1P        | 2021-04-30  |
        | TW1P        | 2021-03-31  |
        | TW1P        | 2021-02-26  |
        | TW1P        | 2021-01-29  |
        | TW1P        | 2020-12-31  |
        | TW1P        | 2020-11-30  |
        