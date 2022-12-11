
def REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT(table_name: str, depth_size: int):
    HEADER = "create table {}".format(table_name)
    REQUIRED_FIELDS = """(
    CHANGE_ID bigint not null,
    NAME_INSTRUMENT blob                           not null,
    TIMESTAMP_VALUE bigint                           not null,
    """
    ADDITIONAL_FIELDS_BIDS = """
    BID_{}_PRICE float not null,
    BID_{}_AMOUNT float not null, 
    """

    ADDITIONAL_FIELDS_ASKS = """
    ASK_{}_PRICE float not null,
    ASK_{}_AMOUNT float not null, 
    """

    LOWER_HEADER = """
    PRIMARY KEY(CHANGE_ID)
    )
    comment 'Таблица содержащая ордербук на момент запуска скрипта'
    """

    REQUEST = HEADER + REQUIRED_FIELDS
    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_BIDS.format(pointer, pointer)

    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_ASKS.format(pointer, pointer)

    REQUEST += LOWER_HEADER

    return REQUEST