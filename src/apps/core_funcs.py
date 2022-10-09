import win32com.client
import pandas as pd
import pyodbc
import warnings


class ExcelData():
    """
    class to load different datasource to excel
    """

    def __init__(self, df=None):
        self.df = df

    def df_to_curr_xl(self, ):

        # Create an instance of the Excel Application & make it visible.
        ExcelApp = win32com.client.GetActiveObject("Excel.Application")
        ExcelApp.Visible = True

        rows, cols = self.df.shape
        headers = self.df.columns.tolist()

        # Take the data frame object and convert it to a recordset array
        rec_array = self.df.to_records(index=False)
        # Convert the Recordset Array to a list.
        # This is because Excel doesn't recognize Numpy datatypes.
        rec_array = rec_array.tolist()

        # set the value property equal to the record array.
        header_start_rng = ExcelApp.ActiveCell.Address
        header_end_rng = ExcelApp.ActiveSheet.Range(
            header_start_rng).resize(1, cols).Address
        header_output_rng = f'{header_start_rng}:{header_end_rng}'

        start_rng = ExcelApp.ActiveSheet.Range(
            header_start_rng).resize(2, 1).Address
        end_rng = ExcelApp.ActiveSheet.Range(
            start_rng).resize(rows, cols).Address
        output_rng = f'{start_rng}:{end_rng}'

        ExcelApp.ActiveSheet.Range(header_output_rng).Value = headers
        ExcelApp.ActiveSheet.Range(output_rng).Value = rec_array

    def load_data_file(self, file_path):
        self.df = pd.read_csv(file_path)
        return 0

    def load_data_sql(self, sql_path, server='WENJING-DESKTOP\\SQLEXPRESS'):
        conn_str = f'''
            Driver={{SQL Server}};
            Server={server};
            Trusted_Connection=yes;
            '''
        warnings.filterwarnings('ignore')

        conn = pyodbc.connect(conn_str)

        with open(sql_path, 'r') as query:
            self.df = pd.read_sql_query(query.read(), conn)

        return 0


def main():
    pass


if __name__ == '__main__':
    main()
