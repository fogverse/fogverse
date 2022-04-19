import sys

import numpy as np

from io import BytesIO

size = sys.getsizeof

def bytes_to_numpy(bbytes):
    f = BytesIO(bbytes)
    return np.load(f, allow_pickle=True)

def numpy_to_bytes(arr):
    f = BytesIO()
    np.save(f,arr)
    return f.getvalue()

def size_kb(s):
    _size = size(s)
    return round(_size/1e3, 2)
