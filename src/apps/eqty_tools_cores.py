from dataclasses import dataclass
import pyodbc

@dataclass
class StatConfig():
    results_server: str
    results_db: str
    output_file: str


@dataclass
class SqlConnection():
    server: str
    db: str

    def get_connection_string(self, ) -> str:
        pass

    def connect(self, ):
        conn_str = self.get_connection_string()
        conn = pyodbc.connect(conn_str)
        print(f'{self.server}.{self.db} connected!')
        return conn


def main():
    pass


if __name__ == '__main__':
    main()
