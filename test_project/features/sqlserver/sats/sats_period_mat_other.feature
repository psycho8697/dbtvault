@fixture.set_workdir
Feature: Satellites Loaded using Period Materialization for other scenarios (sqlserver)

  # ABORTED LOADS

  @fixture.satellite_cycle_sqlserver
  Scenario: [SAT-PERIOD-MAT] Simulate a restart of an aborted load
    Given the RAW_STAGE stage is empty
    And the SATELLITE sat is already populated with data
      | CUSTOMER_PK | HASHDIFF                             | CUSTOMER_NAME | CUSTOMER_DOB | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1001') | md5('1990-02-03\|\|1001\|\|ALBERT')  | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BETH')    | Beth          | 1995-08-07   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHARLEY') | Charley       | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
    When the RAW_STAGE is loaded
      | CUSTOMER_ID | CUSTOMER_NAME | CUSTOMER_DOB | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | 1001        | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1002        | Beth          | 1995-08-07   | 2019-05-04     | 2019-05-04 | *      |
      | 1003        | Charley       | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1010        | Jenny         | 1991-03-21   | 2019-05-04     | 2019-05-04 | *      |
      | 1012        | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1002        | Beah          | 1995-08-07   | 2019-05-05     | 2019-05-05 | *      |
      | 1003        | Chris         | 1990-02-03   | 2019-05-05     | 2019-05-05 | *      |
      | 1004        | David         | 1992-01-30   | 2019-05-05     | 2019-05-05 | *      |
      | 1010        | Jenny         | 1991-03-25   | 2019-05-05     | 2019-05-05 | *      |
      | 1002        | Beth          | 1995-08-07   | 2019-05-06     | 2019-05-06 | *      |
      | 1003        | Claire        | 1990-02-03   | 2019-05-06     | 2019-05-06 | *      |
      | 1005        | Elwyn         | 2001-07-23   | 2019-05-06     | 2019-05-06 | *      |
      | 1006        | Freia         | 1960-01-01   | 2019-05-06     | 2019-05-06 | *      |
      | 1002        | Beah          | 1995-08-07   | 2019-05-07     | 2019-05-07 | *      |
      | 1003        | Charley       | 1990-02-03   | 2019-05-07     | 2019-05-07 | *      |
      | 1007        | Geoff         | 1990-02-03   | 2019-05-07     | 2019-05-07 | *      |
      | 1010        | Jenny         | 1991-03-25   | 2019-05-07     | 2019-05-07 | *      |
      | 1011        | Karen         | 1978-06-16   | 2019-05-07     | 2019-05-07 | *      |
    And I create the STG_CUSTOMER stage
    And I insert by period into the SATELLITE sat by day
    Then the SATELLITE table should contain expected data
      | CUSTOMER_PK | HASHDIFF                             | CUSTOMER_NAME | CUSTOMER_DOB | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1001') | md5('1990-02-03\|\|1001\|\|ALBERT')  | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BETH')    | Beth          | 1995-08-07   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHARLEY') | Charley       | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1010') | md5('1991-03-21\|\|1010\|\|JENNY')   | Jenny         | 1991-03-21   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1012') | md5('1990-02-03\|\|1012\|\|ALBERT')  | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BEAH')    | Beah          | 1995-08-07   | 2019-05-05     | 2019-05-05 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHRIS')   | Chris         | 1990-02-03   | 2019-05-05     | 2019-05-05 | *      |
      | md5('1004') | md5('1992-01-30\|\|1004\|\|DAVID')   | David         | 1992-01-30   | 2019-05-05     | 2019-05-05 | *      |
      | md5('1010') | md5('1991-03-25\|\|1010\|\|JENNY')   | Jenny         | 1991-03-25   | 2019-05-05     | 2019-05-05 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BETH')    | Beth          | 1995-08-07   | 2019-05-06     | 2019-05-06 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CLAIRE')  | Claire        | 1990-02-03   | 2019-05-06     | 2019-05-06 | *      |
      | md5('1005') | md5('2001-07-23\|\|1005\|\|ELWYN')   | Elwyn         | 2001-07-23   | 2019-05-06     | 2019-05-06 | *      |
      | md5('1006') | md5('1960-01-01\|\|1006\|\|FREIA')   | Freia         | 1960-01-01   | 2019-05-06     | 2019-05-06 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BEAH')    | Beah          | 1995-08-07   | 2019-05-07     | 2019-05-07 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHARLEY') | Charley       | 1990-02-03   | 2019-05-07     | 2019-05-07 | *      |
      | md5('1007') | md5('1990-02-03\|\|1007\|\|GEOFF')   | Geoff         | 1990-02-03   | 2019-05-07     | 2019-05-07 | *      |
      | md5('1011') | md5('1978-06-16\|\|1011\|\|KAREN')   | Karen         | 1978-06-16   | 2019-05-07     | 2019-05-07 | *      |

  # INFERRED DATE RANGE (MONTHLY)

  @fixture.satellite_cycle_sqlserver
  Scenario: [SAT-PERIOD-MAT] Satellite load over several monthly cycles with insert_by_period into empty satellite.
    Given the RAW_STAGE stage is empty
    And the SATELLITE sat is empty
    When the RAW_STAGE is loaded
      | CUSTOMER_ID | CUSTOMER_NAME | CUSTOMER_DOB | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | 1001        | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1002        | Beth          | 1995-08-07   | 2019-05-04     | 2019-05-04 | *      |
      | 1003        | Charley       | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1010        | Jenny         | 1991-03-21   | 2019-05-04     | 2019-05-04 | *      |
      | 1012        | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | 1002        | Beah          | 1995-08-07   | 2019-06-05     | 2019-06-05 | *      |
      | 1003        | Chris         | 1990-02-03   | 2019-06-05     | 2019-06-05 | *      |
      | 1004        | David         | 1992-01-30   | 2019-06-05     | 2019-06-05 | *      |
      | 1010        | Jenny         | 1991-03-25   | 2019-06-05     | 2019-06-05 | *      |
      | 1002        | Beth          | 1995-08-07   | 2019-07-06     | 2019-07-06 | *      |
      | 1003        | Claire        | 1990-02-03   | 2019-07-06     | 2019-07-06 | *      |
      | 1005        | Elwyn         | 2001-07-23   | 2019-07-06     | 2019-07-06 | *      |
      | 1006        | Freia         | 1960-01-01   | 2019-07-06     | 2019-07-06 | *      |
      | 1002        | Beah          | 1995-08-07   | 2019-08-07     | 2019-08-07 | *      |
      | 1003        | Charley       | 1990-02-03   | 2019-08-07     | 2019-08-07 | *      |
      | 1007        | Geoff         | 1990-02-03   | 2019-08-07     | 2019-08-07 | *      |
      | 1010        | Jenny         | 1991-03-25   | 2019-08-07     | 2019-08-07 | *      |
      | 1011        | Karen         | 1978-06-16   | 2019-08-07     | 2019-08-07 | *      |
    And I create the STG_CUSTOMER stage
    And I insert by period into the SATELLITE sat by month
    Then the SATELLITE table should contain expected data
      | CUSTOMER_PK | HASHDIFF                             | CUSTOMER_NAME | CUSTOMER_DOB | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1001') | md5('1990-02-03\|\|1001\|\|ALBERT')  | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BETH')    | Beth          | 1995-08-07   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHARLEY') | Charley       | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1010') | md5('1991-03-21\|\|1010\|\|JENNY')   | Jenny         | 1991-03-21   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1012') | md5('1990-02-03\|\|1012\|\|ALBERT')  | Albert        | 1990-02-03   | 2019-05-04     | 2019-05-04 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BEAH')    | Beah          | 1995-08-07   | 2019-06-05     | 2019-06-05 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHRIS')   | Chris         | 1990-02-03   | 2019-06-05     | 2019-06-05 | *      |
      | md5('1004') | md5('1992-01-30\|\|1004\|\|DAVID')   | David         | 1992-01-30   | 2019-06-05     | 2019-06-05 | *      |
      | md5('1010') | md5('1991-03-25\|\|1010\|\|JENNY')   | Jenny         | 1991-03-25   | 2019-06-05     | 2019-06-05 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BETH')    | Beth          | 1995-08-07   | 2019-07-06     | 2019-07-06 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CLAIRE')  | Claire        | 1990-02-03   | 2019-07-06     | 2019-07-06 | *      |
      | md5('1005') | md5('2001-07-23\|\|1005\|\|ELWYN')   | Elwyn         | 2001-07-23   | 2019-07-06     | 2019-07-06 | *      |
      | md5('1006') | md5('1960-01-01\|\|1006\|\|FREIA')   | Freia         | 1960-01-01   | 2019-07-06     | 2019-07-06 | *      |
      | md5('1002') | md5('1995-08-07\|\|1002\|\|BEAH')    | Beah          | 1995-08-07   | 2019-08-07     | 2019-08-07 | *      |
      | md5('1003') | md5('1990-02-03\|\|1003\|\|CHARLEY') | Charley       | 1990-02-03   | 2019-08-07     | 2019-08-07 | *      |
      | md5('1007') | md5('1990-02-03\|\|1007\|\|GEOFF')   | Geoff         | 1990-02-03   | 2019-08-07     | 2019-08-07 | *      |
      | md5('1011') | md5('1978-06-16\|\|1011\|\|KAREN')   | Karen         | 1978-06-16   | 2019-08-07     | 2019-08-07 | *      |