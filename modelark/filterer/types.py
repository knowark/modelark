from typing import Tuple, Union, Sequence


TermTuple = Tuple[str, str, Union[str, int, float, bool, list, tuple]]

QueryDomain = Sequence[Union[str, TermTuple]]
