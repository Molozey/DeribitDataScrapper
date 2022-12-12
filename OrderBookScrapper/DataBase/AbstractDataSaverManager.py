import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pandas import DataFrame
import os
import logging
import json
import yaml
import numpy as np

from OrderBookScrapper.Scrappers.AbstractSubscription import AbstractSubscription, OrderBookSubscription

class AutoIncrementDict(dict):
    pointer = -1

    def __init__(self, path_to_file):
        super().__init__()
        if not os.path.exists(path_to_file):
            logging.info("No cached instruments map exist")
            self.add_instrument(key="EMPTY-INSTRUMENT")

        else:
            logging.info("Cache instruments map exist")
            self.download_cache_from_file(path_to_file=path_to_file)

    def download_cache_from_file(self, path_to_file: str):
        # Load existed instrument map
        with open(path_to_file, "r") as _file:
            instrument_name_instrument_id_map = json.load(_file)

        for objects in instrument_name_instrument_id_map.items():
            self.add_instrument(key=objects[0], value=objects[1])

        self.pointer = max(instrument_name_instrument_id_map.values())

        logging.info(f"Dict map has last pointer equals to {self.pointer}")

    def add_instrument(self, key, value=None):
        if not value:
            self.pointer += 1
            super().__setitem__(key, self.pointer)
        else:
            super().__setitem__(key, value)


class AbstractDataManager(ABC):
    instrument_name_instrument_id_map: AutoIncrementDict[str, int] = None
    circular_batch_tables: Dict[int, DataFrame]

    batch_mutable_pointer: Optional[int] = None
    batch_number_of_tables: Optional[int] = None
    batch_currently_selected_table: Optional[int] = None
    async_loop = asyncio.new_event_loop()

    subscription_type: Optional[AbstractSubscription] = None
    def __init__(self, config_path, subscription_type: Optional[AbstractSubscription] = OrderBookSubscription):
        # Config file
        with open(config_path, "r") as ymlfile:
            self.cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

        constant_depth_mode = self.cfg["orderBookScrapper"]["depth"]
        if type(constant_depth_mode) == int:
            self.depth_size = constant_depth_mode
        elif constant_depth_mode is False:
            self.depth_size = 0
        else:
            raise ValueError("Error in depth")

        # Download instrument hashMap
        self.instrument_name_instrument_id_map = AutoIncrementDict(path_to_file=
                                                                   self.cfg["record_system"][
                                                                       "instrumentNameToIdMapFile"])

        # Check if all structure and content of record system is correct
        self.async_loop.run_until_complete(self.async_loop.create_task(self._connect_to_database()))
        self.async_loop.run_until_complete(self.async_loop.create_task(self._validate_existing_of_database_structure()))
        # Create tmp_storages
        self._create_tmp_batch_tables()

    async def _validate_existing_of_database_structure(self):
        """
        Validate correct structure of record system. Check if all need tables are exist.
        :return:
        """
        # Check if storages exists
        if self.cfg["orderBookScrapper"]["enable_database_record"]:
            await self._create_not_exist_database()
        else:
            logging.warning("Selected no record system")

        if self.cfg["record_system"]["clean_database_at_startup"]:
            await self._clean_exist_database()

        return None

    @abstractmethod
    async def _connect_to_database(self):
        pass

    @abstractmethod
    async def _clean_exist_database(self):
        pass

    @abstractmethod
    async def _create_not_exist_database(self):
        pass

    @abstractmethod
    def _add_order_book_content_limited_depth(self, bids, asks, change_id, timestamp, instrument_name):
        pass

    @abstractmethod
    def _add_instrument_change_order_book_unlimited_depth(self, request_change_id: int, request_previous_change_id: int,
                                                          change_timestamp: int,
                                                          bids_list: list[list[str, float, float]],
                                                          asks_list: list[list[str, float, float]]
                                                          ):
        pass

    @abstractmethod
    def _add_instrument_init_snapshot(self, instrument_name: str,
                                      start_instrument_scrap_time: int,
                                      request_change_id: int,
                                      bids_list,
                                      asks_list: list[list[str, float, float]]
                                      ):
        pass

    # TODO: dynamic creation for extra fields.
    def _create_tmp_batch_tables(self):
        """
        Creates tmp batches for batch record system.
        :return:
        """
        columns = ["CHANGE_ID", "NAME_INSTRUMENT", "TIMESTAMP_VALUE"]
        columns.extend(map(lambda x: [f"BID_{x}_PRICE", f"BID_{x}_AMOUNT"], range(self.depth_size)))
        columns.extend(map(lambda x: [f"ASK_{x}_PRICE", f"ASK_{x}_AMOUNT"], range(self.depth_size)))

        columns = flatten(columns)
        self.batch_mutable_pointer = 0
        self.batch_currently_selected_table = 0

        # Create columns for tmp tables
        if self.cfg["record_system"]["use_batches_to_record"]:
            _local = np.zeros(shape=(self.cfg["record_system"]["size_of_tmp_batch_table"], self.depth_size * 4 + 3))
            _local[:] = np.NaN
            # Create tmp tables
            self.circular_batch_tables = {_: DataFrame(_local, columns=columns)
                                          for _ in range(self.cfg["record_system"]["number_of_tmp_tables"])}

            assert len(self.circular_batch_tables) == self.cfg["record_system"]["number_of_tmp_tables"]

            # print(self.circular_batch_tables[self.batch_currently_selected_table])
            del _local, columns
            logging.info(f"""
            TMP tables for batching has been created. Number of tables = ({len(self.circular_batch_tables)}),
            Size of one table is ({self.circular_batch_tables[0].shape})  
            """)
        # No batch system enabled
        else:
            _local = np.zeros(shape=(1, self.depth_size * 4 + 3))
            _local[:] = np.NaN
            # Create tmp tables
            self.circular_batch_tables = {0: DataFrame(_local, columns=columns)}

            # print(self.circular_batch_tables[self.batch_currently_selected_table])
            del _local, columns
            logging.info(f"""
            NO BATCH MODE: TMP tables for batching has been created. Number of tables = ({len(self.circular_batch_tables)}),
            Size of one table is ({self.circular_batch_tables[0].shape})  
            """)

    @abstractmethod
    def _record_to_database_limited_depth_mode(self, record_dataframe: DataFrame):
        pass

    @abstractmethod
    def _record_to_database_unlimited_depth_mode(self, record_dataframe: DataFrame):
        pass
