import sys

import numpy as np

from io import BytesIO

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
