
from typing import Dict
from typing import List
from typing import Tuple
from abc import ABC, abstractmethod
from decimal import *

class BaseCatalog(ABC):

    chunksize = 1000

    def __init__(self):
        getcontext().prec = 6

    def readData(self, filename: str=None, colnames: List[str]=None, colspecs: List[Tuple[int,int]]=None) -> Dict:
        pass

    @abstractmethod
    def convert_row_to_dict(self, row) -> Dict:
        pass

    @staticmethod
    def convert_ra_to_decimal() -> Decimal:
        pass

    @staticmethod
    def convert_dec_to_decimal() -> Decimal:
        pass

    def unabbreviate_greek_letter() -> str:
        pass