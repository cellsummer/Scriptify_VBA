from core_funcs import ExcelData
import pandas as pd
import pytest


@pytest.fixture
def excel_data():
    return ExcelData(pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]}))


def test_data_has_two_cols(excel_data):
    assert len(excel_data.df.columns) == 3
