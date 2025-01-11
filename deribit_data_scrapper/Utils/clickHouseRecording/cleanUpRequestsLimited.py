
def REQUEST_TO_CREATE_TRADES_TABLE(table_name: str):
    HEADER = "create table {}".format(table_name)
    REQUEST = HEADER
    REQUEST += """
(
    CHANGE_ID       UUID DEFAULT generateUUIDv4() NOT NULL,
    TIMESTAMP_VALUE Int64 NOT NULL,
    TRADE_ID Float32 NULL,
    PRICE Float32 NULL,
    INSTRUMENT_INDEX TinyInt NULL,
    INSTRUMENT_STRIKE Float32 NULL,
    INSTRUMENT_MATURITY Float32 NULL,
    INSTRUMENT_TYPE Float32 NULL,
    DIRECTION TinyInt NULL,
    AMOUNT Float32 NULL
) ENGINE = MergeTree()
ORDER BY TIMESTAMP_VALUE;
"""
    return REQUEST


def REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT(table_name: str, depth_size: int):
    HEADER = "create table {}".format(table_name)
    REQUIRED_FIELDS = """(
    CHANGE_ID UUID DEFAULT generateUUIDv4() NOT NULL,
    INSTRUMENT_INDEX TinyInt NULL,
    INSTRUMENT_STRIKE Float32 NULL,
    INSTRUMENT_MATURITY Float32 NULL,
    INSTRUMENT_TYPE Float32 NULL,
    TIMESTAMP_VALUE Int64 NOT NULL,
    """
    ADDITIONAL_FIELDS_BIDS = """
    BID_{}_PRICE Float32 not null,
    BID_{}_AMOUNT Float32 not null, 
    """

    ADDITIONAL_FIELDS_ASKS = """
    ASK_{}_PRICE Float32 not null,
    ASK_{}_AMOUNT Float32 not null,"""

    LOWER_HEADER = """
    ) ENGINE = MergeTree()
ORDER BY TIMESTAMP_VALUE
COMMENT 'Test Table';
    """

    REQUEST = HEADER + REQUIRED_FIELDS
    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_BIDS.format(pointer, pointer)

    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_ASKS.format(pointer, pointer)

    REQUEST = REQUEST[:-1]
    REQUEST += LOWER_HEADER

    return REQUEST



if __name__ == '__main__':
    sql = REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT(table_name='sd', depth_size=10)
    print(sql)