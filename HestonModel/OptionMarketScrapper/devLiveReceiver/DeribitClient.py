import time

from websocket import WebSocketApp, enableTrace, ABNF
from threading import Thread

from datetime import datetime
import logging
import json

import MSG_LIST


class DeribitClient(Thread, WebSocketApp):
    websocket: WebSocketApp
    def __init__(self, test_mode: bool = False, enable_traceback: bool = True):
        Thread.__init__(self)
        self.testMode = test_mode
        self.exchange_version = self._set_exchange()
        self.time = datetime.now()

        self.websocket = None
        self.enable_traceback = enable_traceback
        # Set logger settings
        logging.basicConfig(
            level='INFO',
            format='%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def _set_exchange(self):
        if self.testMode:
            return 'wss://test.deribit.com/ws/api/v2'
        else:
            return 'wss://www.deribit.com/ws/api/v2/'

    def run(self):
        self.websocket = WebSocketApp(self.exchange_version,
                                      on_message=self._on_message, on_open=self._on_open, on_error=self._on_error)
        if self.enable_traceback:
            enableTrace(True)
        # Run forever loop
        while True:
            try:
                self.websocket.run_forever()
            except:
                logging.error("Error at run_forever loop")
                # TODO: place here notificator
                continue

    def _on_error(self, websocket, error):
        print(error)
        pass

    def _on_message(self, websocket, message):
        """
        Логика реакции на ответ сервера.
        :param websocket:
        :param message:
        :return:
        """
        response = json.loads(message)
        self._process_callback(response)
        # Answer to heartbeat request
        if 'method' in response:
            if response['method'] == 'heartbeat':
                # Send test message to approve that connection is still alive
                self.send_new_request(MSG_LIST.test_message())


    def _process_callback(self, response):
        logging.info(response)
        pass

    def _on_open(self, websocket):
        logging.info("Client start his work")
        print('start on message')
        self.websocket.send(json.dumps(MSG_LIST.hello_message()))

    def send_new_request(self, request: dict):
        self.websocket.send(json.dumps(request), ABNF.OPCODE_TEXT)


if __name__ == '__main__':
    deribitWorker = DeribitClient(test_mode=True, enable_traceback=False)
    deribitWorker.start()
    # Very important time sleep. I spend smth around 3 hours to understand why my connection
    # is closed when i try to place new request :(
    time.sleep(1)
    # Send Hello Message
    deribitWorker.send_new_request(MSG_LIST.hello_message())
    # Set heartbeat
    deribitWorker.send_new_request(MSG_LIST.set_heartbeat(10))