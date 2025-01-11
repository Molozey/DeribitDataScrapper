import asyncio
import logging
import os
import pandas as pd
import signal
from datetime import datetime
from typing import Optional

import clickhouse_connect.driver
import mysql.connector as connector

from deribit_data_scrapper.DataBase.AbstractDataSaverManager import AbstractDataManager
from deribit_data_scrapper.Subsciption.AbstractSubscription import AbstractSubscription
from deribit_data_scrapper.Utils import *


class ClickHouseDaemon(AbstractDataManager):
    """
    Daemon for ClickHouse record type.
    """

    connection: clickhouse_connect.driver.AsyncClient

    def __init__(
        self,
        configuration_path,
        subscription_type: Optional[AbstractSubscription],
        loop: asyncio.unix_events.SelectorEventLoop,
    ):
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s | %(levelname)s %(module)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        super().__init__(
            config_path=configuration_path,
            subscription_type=subscription_type,
            loop=loop,
        )


    async def _connect_to_database(self):
        """
        Connection to ClickHouseDatabase
        :return:
        """
        try:
            self.connection = await clickhouse_connect.get_async_client(
                host=self.cfg["clickhouse"]["host"],
                port=self.cfg["clickhouse"]["port"],
                user=self.cfg["clickhouse"]["user"],
                password=self.cfg["clickhouse"]["password"],
                database=self.cfg["clickhouse"]["database"],
            )
            print(self.connection)
        except Exception as e:
            logging.error("Cannot connect to ClickHouse. Reached maximum attempts", exc_info=True)
            os.kill(os.getpid(), signal.SIGUSR1)
            raise ConnectionError(
                "Cannot connect to ClickHouse. Reached maximum attempts"
            )
    async def _clean_exist_database(self):
        """
        Clean ClickHouse database
        :return:
        """
        raise NotImplementedError("Cleanup is not available for ClickHouse Driver")

    async def __clean_up_pipeline(self):
        """
        Query to cleanUP mySQL.
        :return:
        """
        raise NotImplementedError("Cleanup is not available for ClickHouse Driver")

    async def _create_not_exist_database(self):
        """
        Check if all need tables are exiting. If not creates them.
        :return:
        """
        # print("Check All exists", self.connection)
        while not getattr(self, "connection", None):
            await asyncio.sleep(1)
        _all_exist = True
        _query = """SHOW TABLES LIKE '{}'"""
        for table_name, table_creation in zip(
            self.subscription_type.tables_names,
            self.subscription_type.tables_names_creation,
        ):
            try:
                result = await self.connection.query(_query.format(table_name))
                result = result.result_rows
            except Exception as e:
                print(e)
                raise
            if not result:
                logging.warning(f"Table {table_name} NOT exist; Start creating...")
                try:
                    await self.connection.query(table_creation)
                except Exception as e:
                    print(e)
                    print(table_creation)
                    raise
                _all_exist = False

        if _all_exist:
            logging.info("All need tables already exists. That's good!")

    async def __database_one_table_record(self, record_dataframe: DataFrame):
        # from copy import deepcopy
        insert = record_dataframe.copy()
        try:
            if 'CHANGE_ID' in insert.columns:
                insert = insert.drop(["CHANGE_ID"], axis=1)
            # if 'INSTRUMENT_INDEX' in insert:
            #     insert['INSTRUMENT_INDEX'] = insert['INSTRUMENT_INDEX'].astype(int)
            await self.connection.insert_df(self.subscription_type.tables_names[0], insert)
        except Exception as e:
            insert.to_csv("ERROR.csv", index=False)
            logging.error("Error while inserting data to clickhouse", exc_info=True)
            os.kill(os.getpid(), signal.SIGUSR1)


    async def _place_data_to_database(self, record_dataframe: DataFrame):
        await self.__database_one_table_record(record_dataframe=record_dataframe)
