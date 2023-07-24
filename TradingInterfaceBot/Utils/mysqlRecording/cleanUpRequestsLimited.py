def REQUEST_TO_CREATE_OWN_ORDERS_TABLE(table_name: str):
    HEADER = "create table {}".format(table_name)
    REQUEST = HEADER
    REQUEST += \
        """
    (
        CHANGE_ID       bigint not null auto_increment primary key,
        CREATION_TIMESTAMP       bigint   null,
        LAST_UPDATE_TIMESTAMP    bigint   null,
        INSTRUMENT_INDEX char(4) null,
        INSTRUMENT_STRIKE float  null,
        INSTRUMENT_MATURITY bigint null,
        INSTRUMENT_TYPE int null,
        ORDER_TYPE blob null,
        ORDER_STATE blob null,
        ORDER_ID    bigint null,
        FILLED_AMOUNT   float null,
        COMMISSION  float null,
        AVERAGE_PRICE float null,
        PRICE   float null,
        DIRECTION       blob  null,
        AMOUNT          float null
    );
    """
    return REQUEST

def REQUEST_TO_CREATE_TRADES_TABLE(table_name: str):
    HEADER = "create table {}".format(table_name)
    REQUEST = HEADER
    REQUEST += \
    """
(
    CHANGE_ID       bigint not null auto_increment primary key,
    TIMESTAMP_VALUE       bigint   null,
    TRADE_ID        bigint   null,
    PRICE           float null,
    INSTRUMENT_INDEX char(4) null,
    INSTRUMENT_STRIKE float  null,
    INSTRUMENT_MATURITY bigint null,
    INSTRUMENT_TYPE int null,
    DIRECTION       blob  null,
    AMOUNT          float null
);
"""
    return REQUEST


def REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT(table_name: str, depth_size: int):
    HEADER = "create table {}".format(table_name)
    REQUIRED_FIELDS = """(
    CHANGE_ID bigint not null auto_increment primary key,
    INSTRUMENT_INDEX char(4) null,
    INSTRUMENT_STRIKE float  null,
    INSTRUMENT_MATURITY bigint null,
    INSTRUMENT_TYPE int null,
    TIMESTAMP_VALUE bigint                           not null,
    """
    ADDITIONAL_FIELDS_BIDS = """
    BID_{}_PRICE float not null,
    BID_{}_AMOUNT float not null, 
    """

    ADDITIONAL_FIELDS_ASKS = """
    ASK_{}_PRICE float not null,
    ASK_{}_AMOUNT float not null,"""

    LOWER_HEADER = """
    )
    comment 'Test Table';
    """

    REQUEST = HEADER + REQUIRED_FIELDS
    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_BIDS.format(pointer, pointer)

    for pointer in range(depth_size):
        REQUEST += ADDITIONAL_FIELDS_ASKS.format(pointer, pointer)

    REQUEST = REQUEST[:-1]
    REQUEST += LOWER_HEADER

    return REQUEST


def REQUEST_TO_CREATE_USER_PORTFOLIO_TABLE(table_name: str):
    HEADER = "create table {}".format(table_name)
    REQUEST = HEADER
    REQUEST += \
        """
    (
        CHANGE_ID       bigint not null auto_increment primary key,
        CREATION_TIMESTAMP       bigint   null,
        TOTAL_PL    float   null,
        MARGIN_BALANCE float  null,
        MAINTENANCE_MARGIN float null,
        INITIAL_MARGIN float null,
        ESTIMATED_LIQUIDATION_RATIO    float null,
        EQUITY   float null,
        DELTA_TOTAL  float null,
        BALANCE float null,
        AVAILABLE_WITHDRAWAL_FUNDS   float null,
        AVAILABLE_FUNDS       float  null
    );
    """
    return REQUEST