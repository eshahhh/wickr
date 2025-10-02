import pandas as pd
from abc import ABC, abstractmethod

class BaseIndicator(ABC):
    def __init__(self, name):
        self.name = name
        self.parameters = {}

    @abstractmethod
    def calculate(self, df):
        pass

    def validate_data(self, df):
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        return all(col in df.columns for col in required_columns)

    def get_info(self):
        return {
            'name': self.name,
            'parameters': self.parameters,
            'description': self.__doc__
        }
