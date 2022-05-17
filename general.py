import traceback

from confluent_kafka import Message

def convert_list_to_dict(lst):
    return {key: value for key, value in lst}

class Runnable:
    def on_error(self, _):
        traceback.print_exc()

    def process(self, data):
        return data

    def header_to_dict(self, message):
        # set headers from list of tuples to dict
        if isinstance(message, Message):
            headers = message.headers()
            if not isinstance(headers, dict):
                headers = {key: value for key, value in headers}
                message.set_headers(headers)

    def run(self):
        try:
            while True:
                self.message = self.receive()
                if self.message is None: continue

                # kafka and opencv consumer compatibility
                getvalue = getattr(self.message, 'value', None)
                if getvalue is None:
                    value = self.message
                elif callable(getvalue):
                    value = self.message.value()
                else:
                    value = self.message.value

                self.header_to_dict(self.message)
                data = self.decode(value)
                result = self.process(data)
                result_bytes = self.encode(result)
                self.send(result_bytes)
        except Exception as e:
            self.on_error(e)
        finally:
            close_producer = getattr(self, 'close_producer', None)
            if callable(close_producer):
                self.close_producer()

            close_consumer = getattr(self, 'close_consumer', None)
            if callable(close_consumer):
                self.close_consumer()
