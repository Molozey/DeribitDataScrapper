from abc import ABC, abstractmethod
from typing import Dict, Optional
from pandas import DataFrame
import os
import logging
import json
import yaml
import numpy as np


def flatten(list_of_lists):
    if len(list_of_lists) == 0:
        return list_of_lists
    if isinstance(list_of_lists[0], list):
        return flatten(list_of_lists[0]) + flatten(list_of_lists[1:])
    return list_of_lists[:1] + flatten(list_of_lists[1:])


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

    def __init__(self, config_path):
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
                                                                   self.cfg["record_system"]["instrumentNameToIdMapFile"])

        # Check if storages exists
        if self.cfg["orderBookScrapper"]["enable_database_record"]:
            self._create_not_exist_database()
        else:
            logging.warning("Selected no record system")

        if self.cfg["record_system"]["clean_database_at_startup"]:
            self._clean_exist_database()

    @abstractmethod
    def _clean_exist_database(self):
        pass

    @abstractmethod
    def _create_not_exist_database(self):
        pass

    @abstractmethod
    def add_order_book_content_limited_depth(self, bids, asks, change_id, timestamp, instrument_name):
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

    @abstractmethod
    def _query_constant_depth_mode(self):
        pass

    @abstractmethod
    def _query_unlimited_depth_mode(self):
        pass

    def _create_tmp_batch_tables(self):
        # Dict
        self.instrument_name_instrument_id_map = \
            AutoIncrementDict(self.cfg["record_system"]["instrumentNameToIdMapFile"])
        # Create columns for tmp tables
        columns = ["CHANGE_ID", "NAME_INSTRUMENT", "TIMESTAMP_VALUE"]
        columns.extend(map(lambda x: [f"BID_{x}_PRICE", f"BID_{x}_AMOUNT"], range(self.depth_size)))
        columns.extend(map(lambda x: [f"ASK_{x}_PRICE", f"ASK_{x}_AMOUNT"], range(self.depth_size)))

        columns = flatten(columns)
        _local = np.zeros(shape=(self.cfg["record_system"]["size_of_tmp_batch_table"], self.depth_size * 4 + 3))
        _local[:] = np.NaN
        self.batch_mutable_pointer = 0
        self.batch_currently_selected_table = 0
        # Create tmp tables
        self.circular_batch_tables = {_: DataFrame(_local, columns=columns)
                                      for _ in range(self.cfg["record_system"]["number_of_tmp_tables"])}

        assert len(self.circular_batch_tables) == self.cfg["record_system"]["number_of_tmp_tables"]

        print(self.circular_batch_tables[self.batch_currently_selected_table])
        del _local, columns
        logging.info(f"""
        TMP tables for batching has been created. Number of tables = ({len(self.circular_batch_tables)}),
        Size of one table is ({self.circular_batch_tables[0].shape})  
        """)

