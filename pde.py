from core_funcs import ExcelData
import typer
from parse_sql import MetaSql

app = typer.Typer()


@app.command()
def parse_sql(file_path: str):
    MetaSql(file_path).compile()
    print('sql has parsed successfully!')

    return 0


@app.command()
def from_dev_sql(file_path: str):
    data = ExcelData()
    data.load_data_sql(file_path)
    data.df_to_curr_xl()
    print('Data from sql has been pasted into Excel')

    return 0


@app.command()
def from_test_sql(file_path: str):
    data = ExcelData()
    data.load_data_sql(file_path, server='TEST')
    data.df_to_curr_xl()
    print('Data from sql has been pasted into Excel')

    return 0


@app.command()
def from_csv(file_path: str):
    data = ExcelData()
    data.load_data_file(file_path)
    data.df_to_curr_xl()
    print('Data from csv has been pasted into Excel')

    return 0


if __name__ == '__main__':
    app()
