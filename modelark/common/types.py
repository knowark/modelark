from typing import Sequence, List, Dict, Union, Tuple, Any, MutableMapping


Term = Tuple[str, str, Union[str, int, float, bool, list, tuple]]

Domain = Sequence[Union[str, Term]]

DataDict = MutableMapping[str, Any]

RecordList = List[DataDict]
