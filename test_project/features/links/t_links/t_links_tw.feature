Feature: [SF-TLK_TW] Transactional Links
Last tested on 14/09/2021 with time_window_period=month and time_window_value=-6

  @fixture.t_link_tw
  Scenario: [SF-TLK-TW-001] Load a populated Transactional Link, all new data is earlier than the time window
    Given the T_LINK_TW t_link is already populated with data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
    And the RAW_STAGE table contains data
      | CUSTOMER_ID | ORDER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | LOAD_DATE  | SOURCE |
      | 1234        | 1238     | 12345685           | 2021-02-20       | DR   | 3478.50  | 2021-02-22 | SAP    |
      | 1234        | 1239     | 12345686           | 2021-02-20       | DR   | 10.00    | 2021-02-22 | SAP    |
      | 1235        | 1240     | 12345687           | 2021-02-20       | DR   | 1734.65  | 2021-02-22 | SAP    |
      | 1236        | 1241     | 12345688           | 2021-02-20       | DR   | 4832.56  | 2021-02-22 | SAP    |
      | 1237        | 1242     | 12345689           | 2021-02-20       | DR   | 10000.00 | 2021-02-22 | SAP    |
      | 1238        | 1243     | 12345690           | 2021-02-20       | CR   | 6823.55  | 2021-02-22 | SAP    |
      | 1238        | 1244     | 12345691           | 2021-02-20       | CR   | 4578.34  | 2021-02-22 | SAP    |
    And I stage the STG_CUSTOMER data
    When I load the T_LINK_TW t_link
    Then the T_LINK_TW table should contain expected data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|1238\|\|12345685') | md5('1234') | md5('1238') | 12345685           | 2021-02-20       | DR   | 3478.50  | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1234\|\|1239\|\|12345686') | md5('1234') | md5('1239') | 12345686           | 2021-02-20       | DR   | 10.00    | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1235\|\|1240\|\|12345687') | md5('1235') | md5('1240') | 12345687           | 2021-02-20       | DR   | 1734.65  | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1236\|\|1241\|\|12345688') | md5('1236') | md5('1241') | 12345688           | 2021-02-20       | DR   | 4832.56  | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1237\|\|1242\|\|12345689') | md5('1237') | md5('1242') | 12345689           | 2021-02-20       | DR   | 10000.00 | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1238\|\|1243\|\|12345690') | md5('1238') | md5('1243') | 12345690           | 2021-02-20       | CR   | 6823.55  | 2021-02-20     | 2021-02-22 | SAP    |
      | md5('1238\|\|1244\|\|12345691') | md5('1238') | md5('1244') | 12345691           | 2021-02-20       | CR   | 4578.34  | 2021-02-20     | 2021-02-22 | SAP    |

  @fixture.t_link_tw
  Scenario: [SF-TLK-TW-002] Load a populated Transactional Link, all new data is within the time window
    Given the T_LINK_TW t_link is already populated with data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
    And the RAW_STAGE table contains data
      | CUSTOMER_ID | ORDER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | LOAD_DATE  | SOURCE |
      | 1234        | 1238     | 12345685           | 2021-04-20       | DR   | 3478.50  | 2021-04-22 | SAP    |
      | 1234        | 1239     | 12345686           | 2021-04-20       | DR   | 10.00    | 2021-04-22 | SAP    |
      | 1235        | 1240     | 12345687           | 2021-04-20       | DR   | 1734.65  | 2021-04-22 | SAP    |
      | 1236        | 1241     | 12345688           | 2021-04-20       | DR   | 4832.56  | 2021-04-22 | SAP    |
      | 1237        | 1242     | 12345689           | 2021-04-20       | DR   | 10000.00 | 2021-04-22 | SAP    |
      | 1238        | 1243     | 12345690           | 2021-04-20       | CR   | 6823.55  | 2021-04-22 | SAP    |
      | 1238        | 1244     | 12345691           | 2021-04-20       | CR   | 4578.34  | 2021-04-22 | SAP    |
    And I stage the STG_CUSTOMER data
    When I load the T_LINK_TW t_link
    Then the T_LINK_TW table should contain expected data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|1238\|\|12345685') | md5('1234') | md5('1238') | 12345685           | 2021-04-20       | DR   | 3478.50  | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1234\|\|1239\|\|12345686') | md5('1234') | md5('1239') | 12345686           | 2021-04-20       | DR   | 10.00    | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1235\|\|1240\|\|12345687') | md5('1235') | md5('1240') | 12345687           | 2021-04-20       | DR   | 1734.65  | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1236\|\|1241\|\|12345688') | md5('1236') | md5('1241') | 12345688           | 2021-04-20       | DR   | 4832.56  | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1237\|\|1242\|\|12345689') | md5('1237') | md5('1242') | 12345689           | 2021-04-20       | DR   | 10000.00 | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1238\|\|1243\|\|12345690') | md5('1238') | md5('1243') | 12345690           | 2021-04-20       | CR   | 6823.55  | 2021-04-20     | 2021-04-22 | SAP    |
      | md5('1238\|\|1244\|\|12345691') | md5('1238') | md5('1244') | 12345691           | 2021-04-20       | CR   | 4578.34  | 2021-04-20     | 2021-04-22 | SAP    |

  @fixture.t_link_tw
  Scenario: [SF-TLK-TW-003] Load a populated Transactional Link, new data is earlier than the time window and duplicate of pre time window data
    Given the T_LINK_TW t_link is already populated with data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
    And the RAW_STAGE table contains data
      | CUSTOMER_ID | ORDER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | LOAD_DATE  | SOURCE |
      | 1234        | 4321     | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-22 | SAP    |
    And I stage the STG_CUSTOMER data
    When I load the T_LINK_TW t_link
    Then the T_LINK_TW table should contain expected data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-22 | SAP    |

  @fixture.t_link_tw
  Scenario: [SF-TLK-TW-004] Load a populated Transactional Link, new data is within the time window and duplicate of pre time window data
    Given the T_LINK_TW t_link is already populated with data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
    And the RAW_STAGE table contains data
      | CUSTOMER_ID | ORDER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | LOAD_DATE  | SOURCE |
      | 1234        | 4321     | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-04-22 | SAP    |
    And I stage the STG_CUSTOMER data
    When I load the T_LINK_TW t_link
    Then the T_LINK_TW table should contain expected data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-02-19       | CR   | 123.40   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-02-19       | DR   | 2546.23  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-02-19       | CR   | -123.40  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-02-19       | CR   | 37645.34 | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-02-19       | CR   | 236.55   | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-02-19       | DR   | 3567.34  | 2021-02-19     | 2021-02-21 | SAP    |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-02-19       | DR   | 2340.50  | 2021-02-19     | 2021-04-22 | SAP    |

  @fixture.t_link_tw
  Scenario: [SF-TLK-TW-005] Load a populated Transactional Link, new data is within the time window and duplicate of data within the time window
    Given the T_LINK_TW t_link is already populated with data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-04-19       | DR   | 2340.50  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-04-19       | CR   | 123.40   | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-04-19       | DR   | 2546.23  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-04-19       | CR   | -123.40  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-04-19       | CR   | 37645.34 | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-04-19       | CR   | 236.55   | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-04-19       | DR   | 3567.34  | 2021-04-19     | 2021-04-21 | SAP    |
    And the RAW_STAGE table contains data
      | CUSTOMER_ID | ORDER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | LOAD_DATE  | SOURCE |
      | 1234        | 4321     | 12345678           | 2021-04-19       | DR   | 2340.50  | 2021-04-22 | SAP    |
    And I stage the STG_CUSTOMER data
    When I load the T_LINK_TW t_link
    Then the T_LINK_TW table should contain expected data
      | TRANSACTION_PK                  | CUSTOMER_FK | ORDER_FK    | TRANSACTION_NUMBER | TRANSACTION_DATE | TYPE | AMOUNT   | EFFECTIVE_FROM | LOAD_DATE  | SOURCE |
      | md5('1234\|\|4321\|\|12345678') | md5('1234') | md5('4321') | 12345678           | 2021-04-19       | DR   | 2340.50  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4322\|\|12345679') | md5('1234') | md5('4322') | 12345679           | 2021-04-19       | CR   | 123.40   | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4323\|\|12345680') | md5('1234') | md5('4323') | 12345680           | 2021-04-19       | DR   | 2546.23  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1234\|\|4324\|\|12345681') | md5('1234') | md5('4324') | 12345681           | 2021-04-19       | CR   | -123.40  | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1235\|\|4325\|\|12345682') | md5('1235') | md5('4325') | 12345682           | 2021-04-19       | CR   | 37645.34 | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1236\|\|4326\|\|12345683') | md5('1236') | md5('4326') | 12345683           | 2021-04-19       | CR   | 236.55   | 2021-04-19     | 2021-04-21 | SAP    |
      | md5('1237\|\|4327\|\|12345684') | md5('1237') | md5('4327') | 12345684           | 2021-04-19       | DR   | 3567.34  | 2021-04-19     | 2021-04-21 | SAP    |
