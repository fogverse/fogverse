import inspect
import logging
import time
import pandas as pd

from .formatter import CsvFormatter
from .handler import CsvRotatingFileHandler
from .base import AbstractLogging

from fogverse.util import calc_datetime, get_header, get_timestamp, size_kb

from aiokafka import ConsumerRecord
from os import makedirs, path
from pathlib import Path

DEFAULT_FMT = '%(levelname)s | %(name)s | %(message)s'

def _get_logger(name=None,
                level=None,
                handlers=[],
                formatter=None):
    logger = logging.getLogger(name)
    logger.setLevel(level or logging.NOTSET)

    if not handlers:
        handler = logging.StreamHandler()
        formatter = formatter or logging.Formatter(fmt=DEFAULT_FMT)
        handler.setFormatter(formatter)

        handlers = [handler]

    if type(handlers) not in (list,tuple): handlers = [handlers]
    for h in handlers:
        logger.addHandler(h)
    return logger

def _calc_delay(start, end=None, decimals=2):
    end = end or time.time()
    delay = (end - start) * 1E3
    return round(delay, decimals)

class BaseLogging(AbstractLogging):
    def __init__(self,
                 name=None,
                 level=logging.INFO,
                 handler=None,
                 formatter=None):
        if name is None:
            name = self.__class__.__name__
        self._log = _get_logger(name=name,
                                level=level,
                                handlers=handler,
                                formatter=formatter)

class DefaultLogging(BaseLogging):
    def _before_start(self):
        self._log.info('Starting')

    def _after_start(self):
        self._log.info('The consumer and/or producer have started')

    def _before_receive(self):
        self._log.debug('='*30)
        self._start = time.time()

    def _after_receive(self, data):
        delay = _calc_delay(self._start)
        self._log.debug('Consume time %s ms', delay)

        if not isinstance(data, ConsumerRecord): return
        creation_delay = _calc_delay(data.timestamp/1e3)
        self._log.debug('Data received: %s %s %s %s %s %s ms',
                        data.topic, data.partition, data.offset,
                        data.key, data.headers, creation_delay)

    def _before_process(self, value):
        size = size_kb(value)
        self._log.debug('Data received %s KB', size)
        self._before_process_time = time.time()

    def _after_process(self, _):
        delay = _calc_delay(self._before_process_time)
        self._log.debug('Process time %s ms', delay)

    def _after_send(self, data):
        self._log.debug('Sent data %s KB', size_kb(data))

    def _before_close(self):
        self._log.info('Closing')

    def _after_close(self):
        self._log.info('The consumer and/or producer have closed')

class CsvLogging(BaseLogging):
    def __init__(self,
                 name=None,
                 level=logging.DEBUG,
                 handler=None,
                 formatter=None,
                 dirname=None, # locate the file relative to dirname
                 filename=None,
                 mode='w',
                 fmt=None,
                 delimiter=',',
                 datefmt='%Y/%m/%d %H:%M:%S',
                 csv_header=['asctime','name'],
                 df_header=['topic from','topic to','frame','offset received',
                            'frame delay','msg creation delay','consume time',
                            'size data received','process time',
                            'size data sent','send time','offset sent'],
                 add_header=[]):
        self._df_header = df_header
        csv_header.extend(df_header)
        csv_header.extend(add_header)
        if fmt is None:
            fmt = f'%(asctime)s.%(msecs)03d{delimiter}%(name)s{delimiter}%(message)s'
        if filename is None:
            filename = f'logs/log_{self.__class__.__name__}.csv'
        if dirname is None:
            dirname = Path(inspect.getfile(self.__class__)).resolve().parent
        filename = Path(dirname).resolve() / filename
        # make log file directories
        _dirname = path.dirname(filename)
        if _dirname: makedirs(_dirname, exist_ok=True)
        if not handler:
            handler = CsvRotatingFileHandler(filename,
                                             fmt=fmt,
                                             datefmt=datefmt,
                                             header=csv_header,
                                             delimiter=delimiter,
                                             mode=mode)
            formatter = CsvFormatter(fmt=fmt,
                                     datefmt=datefmt,
                                     delimiter=delimiter)
            handler.setFormatter(formatter)
        super().__init__(name=name,level=level,handler=handler,
                         formatter=formatter)
        self._log_data = pd.DataFrame()

    def _before_receive(self):
        self._log_data.drop(self._log_data.index, inplace=True)
        self._start = get_timestamp()

    def _after_receive(self, _):
        delay_consume = calc_datetime(self._start)
        self._log_data['consume time'] = [delay_consume]

    def _after_decode(self, _):
        if isinstance(self.message, ConsumerRecord):
            now = get_timestamp()
            frame_creation_time = get_header(self.message.headers,
                                               'timestamp')
            if frame_creation_time == None:
                frame_delay = -1
            else:
                frame_delay = calc_datetime(frame_creation_time, end=now)
            creation_delay = _calc_delay(self.message.timestamp/1e3)
            offset_received = self.message.offset
            topic_from = self.message.topic
        else:
            frame_delay = -1
            creation_delay = self._log_data['consume time'][0]
            offset_received = -1
            topic_from = None

        extras = getattr(self, '_message_extra', None)
        if extras:
            consume_time = extras.get('consume time')
            if consume_time:
                self._log_data['consume time'] = [consume_time]
        self._log_data['frame delay'] = [frame_delay]
        self._log_data['msg creation delay'] = [creation_delay]
        self._log_data['offset received'] = [offset_received]
        self._log_data['topic from'] = [topic_from]

    def _before_process(self, value):
        size_received = size_kb(value)
        self._log_data['size data received'] = [size_received]
        self._before_process_time = get_timestamp()

    def _after_process(self, _):
        delay_process = calc_datetime(self._before_process_time)
        self._log_data['process time'] = [delay_process]

    def _before_send(self, data):
        size_sent = size_kb(data)
        self._log_data['size data sent'] = [size_sent]
        self._datetime_before_send = get_timestamp()

    def _get_extra_callback_args(self):
        args, kwargs = [] , {
            'log_data': self._log_data.copy(),
            'headers': getattr(self,'_headers',None),
            'topic': getattr(self,'_topic',None),
            'timestamp': self._datetime_before_send,
        }
        self._log_data.drop(self._log_data.index, inplace=True)
        return args, kwargs

    def callback(self, record_metadata, *args,
                 log_data=None, headers=None, topic=None,
                    timestamp=None, **kwargs):
        frame = int(get_header(headers,'frame',default=-1))
        log_data['offset sent'] = [record_metadata.offset]
        log_data['frame'] = [frame]
        log_data['topic to'] = [topic]
        log_data['send time'] = [calc_datetime(timestamp)]
        data = log_data[self._df_header].iloc[0]
        self._log.debug(data)

    def _after_send(self, data):
        if self._log_data.empty: return
        send_time = calc_datetime(self._datetime_before_send)
        self._log_data['send time'] = [send_time]
        for head in self._df_header:
            if head in self._log_data.columns: continue
            self._log_data[head] = None
        data = self._log_data[self._df_header].iloc[0]
        self._log.debug(data)
