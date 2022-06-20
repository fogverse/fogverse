import os
import sys
import base64
import uuid
import cv2
import numpy as np

from datetime import datetime
from io import BytesIO

def get_cam_id():
    return f"cam_{os.getenv('CAM_ID', str(uuid.uuid4()))}"

def bytes_to_numpy(bbytes):
    f = BytesIO(bbytes)
    return np.load(f, allow_pickle=True)

def numpy_to_bytes(arr):
    f = BytesIO()
    np.save(f,arr)
    return f.getvalue()

def size_kb(s, decimals=2):
    _size = sys.getsizeof(s)
    return round(_size/1e3, decimals)

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

def get_timestamp(utc=True):
    if utc:
        _date = datetime.utcnow()
    else:
        _date = datetime.now()
    return _date

def get_timestamp_str(date=None, utc=True, format=DATETIME_FORMAT):
    date = date or get_timestamp(utc=utc)
    return datetime.strftime(date, format)

def timestamp_to_datetime(timestamp, format=DATETIME_FORMAT):
    if isinstance(timestamp, bytes):
        timestamp = timestamp.decode()
    return datetime.strptime(timestamp, format)

def calc_datetime(start, end=None, format=DATETIME_FORMAT, decimals=2,
                  utc=True):
    if end is None:
        end = get_timestamp(utc=utc)
    elif isinstance(end, str):
        end = datetime.strptime(end, format)
    if isinstance(start, str):
        start = datetime.strptime(start, format)
    diff = (end - start).total_seconds()*1e3
    return round(diff, decimals)

def get_header(headers, key, default=None, decoder=None):
    if headers is None or key is None: return default
    for header in headers:
        if header[0] == key:
            val = header[1]
            if callable(decoder):
                return decoder(val)
            if isinstance(val, bytes):
                return val.decode()
            return val
    return default

def _encode(img, encoding):
    _, encoded = cv2.imencode(f'.{encoding}', img)
    return encoded

def compress_encoding(img, encoding):
    encoded = _encode(img, encoding)
    return numpy_to_bytes(encoded)

def _decode(img):
    return cv2.imdecode(img, cv2.IMREAD_COLOR)

def recover_encoding(img_bytes):
    img = bytes_to_numpy(img_bytes)
    return _decode(img)

def numpy_to_base64_url(img, encoding):
    img = _encode(img, encoding)
    b64 = base64.b64encode(img).decode()
    return f'data:image/{encoding};base64,{b64}'
