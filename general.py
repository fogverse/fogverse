import cv2
import traceback

from confluent_kafka import Consumer as _Consumer, Producer as _Producer

class Runnable:
    def on_error(self, _):
        traceback.print_exc()

    def process(self, data):
        return data

    def close(self):
        consumer = getattr(self, 'consumer', None)
        if isinstance(consumer, _Consumer):
            consumer.close()
        elif isinstance(consumer, cv2.VideoCapture):
            consumer.release()

        producer = getattr(self, 'producer', None)
        if isinstance(producer, _Producer):
            producer.flush()
        print('all closed')

    def run(self):
        try:
            while True:
                self.message = self.receive()
                if self.message is None: continue

                # kafka and opencv consumer compatibility
                getvalue = getattr(self.message, 'value', None)
                if callable(getvalue):
                    value = self.message.value()
                else:
                    value = self.message

                data = self.decode(value)
                result = self.process(data)
                result_bytes = self.encode(result)
                self.send(result_bytes)
        except Exception as e:
            self.on_error(e)
        finally:
            self.close()
