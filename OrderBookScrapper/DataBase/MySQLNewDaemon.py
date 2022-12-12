import asyncio
import json

from pandas import DataFrame

from AbstractDataSaverManager import AbstractDataManager
import logging
import mysql.connector as connector
from OrderBookScrapper.DataBase.mysqlRecording.cleanUpRequestsUnlimited import *
from OrderBookScrapper.DataBase.mysqlRecording.cleanUpRequestsLimited import *


class MySqlDaemon(AbstractDataManager):
    """
    Daemon for MySQL record type.
    """
    # Block with naming
    _unlimited_main_table = "order_book_content"
    _unlimited_initial_table = "script_snapshot_id"
    _unlimited_pairs_table = "script_snapshot_id"

    _limited_table_name_template = "TABLE_DEPTH_{}"

    connection: connector.connection.MySQLConnection
    database_cursor: connector.connection.MySQLCursor

    def __init__(self, configuration_path):
        logging.basicConfig(
            level='INFO',
            format='%(asctime)s | %(levelname)s %(module)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        super().__init__(config_path=configuration_path)

    async def _connect_to_database(self):
        """
        Connection to MySQL database
        :return:
        """
        flag = 0
        while flag < self.cfg["mysql"]["reconnect_max_attempts"]:
            if flag >= self.cfg["mysql"]["reconnect_max_attempts"]:
                raise ConnectionError("Cannot connect to MySQL. Reached maximum attempts")

            try:
                self.connection = connector.connect(host=self.cfg["mysql"]["host"],
                                                    user=self.cfg["mysql"]["user"],
                                                    database=self.cfg["mysql"]["database"])
                self.database_cursor = self.connection.cursor()
                logging.info("Success connection to MySQL database")
                return 1
            except connector.Error as e:
                flag += 1
                logging.error("Connection to database raise error: \n {error}".format(error=e))
                await asyncio.sleep(self.cfg["mysql"]["reconnect_wait_time"])

    async def _mysql_post_execution_handler(self, query) -> int:
        """
        Interface to execute POST request to MySQL database
        :param query:
        :return:
        """
        flag = 0
        while flag < self.cfg["mysql"]["reconnect_max_attempts"]:
            if flag >= self.cfg["mysql"]["reconnect_max_attempts"]:
                raise ConnectionError("Cannot execute MySQL query. Reached maximum attempts")
            try:
                self.database_cursor.execute(query)
                return 1
            except connector.Error as e:
                flag += 1
                logging.error("MySQL execution error: \n {error}".format(error=e))
                await asyncio.sleep(self.cfg["mysql"]["reconnect_wait_time"])

    # TODO: typing
    async def _mysql_get_execution_handler(self, query) -> object:
        """
        Interface to execute GET request to MySQL database
        :param query:
        :return:
        """
        flag = 0
        while flag < self.cfg["mysql"]["reconnect_max_attempts"]:
            if flag >= self.cfg["mysql"]["reconnect_max_attempts"]:
                raise ConnectionError("Cannot execute MySQL query. Reached maximum attempts")
            try:
                self.database_cursor.execute(query)
                return self.database_cursor.fetchone()
            except connector.Error as e:
                flag += 1
                logging.error("MySQL execution error: \n {error}".format(error=e))
                await asyncio.sleep(self.cfg["mysql"]["reconnect_wait_time"])

    async def _clean_exist_database(self):
        """
        Clean MySQL database body method
        :return:
        """
        flag = 0
        while flag < self.cfg["mysql"]["reconnect_max_attempts"]:
            if flag >= self.cfg["mysql"]["reconnect_max_attempts"]:
                raise ConnectionError("Cannot connect to MySQL. Reached maximum attempts")
            try:
                await self.__clean_up_pipeline()
                return 0

            except connector.Error as error:
                flag += 1
                logging.warning("Database clean up error! :{}".format(error))
                await asyncio.sleep(self.cfg["mysql"]["reconnect_wait_time"])

    async def __clean_up_pipeline(self):
        """
        Query to cleanUP mySQL.
        :return:
        """
        if self.depth_size == 0:
            _truncate_query = """TRUNCATE table {}""".format(self._unlimited_main_table)
            await self._mysql_post_execution_handler(_truncate_query)
            _truncate_query = """TRUNCATE table {}""".format(self._unlimited_pairs_table)
            await self._mysql_post_execution_handler(_truncate_query)
            _truncate_query = """TRUNCATE table {}""".format(self._unlimited_initial_table)
            await self._mysql_post_execution_handler(_truncate_query)
            del _truncate_query
        # Limited mode
        else:
            _truncate_query = f"""TRUNCATE table {self._limited_table_name_template.format(
                self.depth_size)}"""
            await self._mysql_post_execution_handler(_truncate_query)
            del _truncate_query

    async def _create_not_exist_database(self):
        """
        Check if all need tables are exiting. If not creates them.
        :return:
        """
        # Unlimited mode
        if self.depth_size == 0:
            _all_exist = True
            _query = """SHOW TABLES LIKE '{}'"""
            # script_snapshot_id
            result = await self._mysql_get_execution_handler(_query.format("script_snapshot_id"))
            if not result:
                logging.warning("script_snapshot_id table NOT exist; Start creating...")
                await self._mysql_post_execution_handler(REQUEST_TO_CREATE_SCRIPT_SNAPSHOT_ID)
                _all_exist = False

            # pairs_new_old
            result = await self._mysql_get_execution_handler(_query.format("pairs_new_old"))
            if not result:
                logging.warning("pairs_new_old table NOT exist; Start creating...")
                await self._mysql_post_execution_handler(REQUEST_TO_CREATE_PAIRS_NEW_OLD)
                _all_exist = False

            # order_book_content
            result = await self._mysql_get_execution_handler(_query.format("order_book_content"))
            if not result:
                logging.warning("order_book_content table NOT exist; Start creating...")
                await self._mysql_post_execution_handler(REQUEST_TO_CREATE_ORDER_BOOK_CONTENT)
                _all_exist = False

            if _all_exist:
                logging.info("All need tables already exists. That's good!")

        # Limited mode
        else:
            _all_exist = True
            _table_name = self._limited_table_name_template.format(self.depth_size)
            _query = """SHOW TABLES LIKE '{}'"""
            result = await self._mysql_get_execution_handler(_query.format(_table_name))
            if not result:
                logging.warning("Limited table with depth {} NOT exist; Start creating...".format(self.depth_size))
                await self._mysql_post_execution_handler(
                    REQUEST_TO_CREATE_LIMITED_ORDER_BOOK_CONTENT(_table_name, self.depth_size))
                _all_exist = False

            del _table_name
            if _all_exist:
                logging.info("All need tables already exists. That's good!")

    def _add_order_book_content_limited_depth(self, bids, asks, change_id, timestamp, instrument_name):
        pass

    def _add_instrument_change_order_book_unlimited_depth(self, request_change_id: int, request_previous_change_id: int,
                                                          change_timestamp: int,
                                                          bids_list: list[list[str, float, float]],
                                                          asks_list: list[list[str, float, float]]):
        pass

    def _add_instrument_init_snapshot(self, instrument_name: str, start_instrument_scrap_time: int,
                                      request_change_id: int, bids_list, asks_list: list[list[str, float, float]]):
        pass

    def _record_to_database_limited_depth_mode(self, record_dataframe: DataFrame):
        pass

    def _record_to_database_unlimited_depth_mode(self, record_dataframe: DataFrame):
        pass


if __name__ == '__main__':
    daemon = MySqlDaemon('../configuration.yaml')
    js = "{'jsonrpc': '2.0', 'method': 'subscription', 'params': {'channel': 'book.BTC-PERPETUAL.none.10.100ms', 'data': {'timestamp': 1670796989478, 'instrument_name': 'BTC-PERPETUAL', 'change_id': 52016142177, 'bids': [[17132.0, 35530.0], [17131.5, 64020.0], [17131.0, 20000.0], [17130.5, 1510.0], [17130.0, 30.0], [17129.0, 6000.0], [17128.5, 5250.0], [17127.5, 480.0], [17127.0, 200.0], [17126.5, 4990.0]], 'asks': [[17132.5, 52250.0], [17133.0, 12950.0], [17133.5, 2780.0], [17134.0, 21710.0], [17134.5, 18580.0], [17135.0, 20000.0], [17135.5, 109300.0], [17136.0, 1060.0], [17136.5, 77790.0], [17137.0, 34440.0]]}}}"
    js = js.replace("'", "\"")
    js = json.loads(js)
    print(js)
    print(js['params']['data']['bids'])
    print(daemon.instrument_name_instrument_id_map[js['params']['data']['instrument_name']])

    df = daemon.circular_batch_tables[daemon.batch_currently_selected_table]
    df['CHANGE_ID'] = js['params']['data']['change_id']
    df['NAME_INSTRUMENT'] = daemon.instrument_name_instrument_id_map[js['params']['data']['instrument_name']]

    print(daemon.circular_batch_tables[0].columns)


