from typing import List, MutableMapping, Union, Any


Scalar = Union[int, float, str, bool]

DataDict = MutableMapping[str, Any]

RecordList = List[DataDict]

Value = Union[Scalar, DataDict]
