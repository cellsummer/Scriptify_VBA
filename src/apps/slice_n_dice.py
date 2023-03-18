"""data application to slice and dice data into desired format
"""

# example 1:
#   slice (rows): legal entity
#   dice (columns): base reserve, def reserve, gmdb, etc
#   default function is sum

# example 2:
#   slice (rows): legal entity, stat code
#   dice (columns): separate reserve, fix reserve
#   default function is sum

# Config: slice_n_dice_config.json
from utils.utils import AttributeDict


class Slicer:
    """main slice&dice class"""

    def __init__(self, config: AttributeDict) -> None:
        self.config = config

    def __len__(
        self,
    ):
        return len(self.config["queries"])

    def _construct_sql(self, i: int) -> str:
        """construct sql query based on config
        Args:
            i (int): i-th query defined in config
        Returns:
            str: query string
        """
        slice = self.config["queries"][i]["slice"]
        dice = self.config["queries"][i]["dice"]
        tbl = self.config["cfile_name"]

        # select legal_entity, sum(base_reserve)... from ... group by
        slice_sql = ", ".join(slice)
        dice = [f"SUM({s}) AS {s}" for s in dice]
        dice_sql = ", ".join(dice)

        qry = f"""
        SELECT {slice_sql}, {dice_sql} 
        FROM {tbl}
        GROUP BY {slice_sql}
        ORDER BY {slice_sql}
        """

        return qry

    def _run_sql(self, sql: str) -> None:
        """use sqlcmd to run the sql"""
        pass

    def process(
        self,
    ) -> None:
        """main process function. writes file to output location"""
        for i in range(self.__len__()):
            sql = self._construct_sql(i)
            print(sql)
            self._run_sql(sql)


def slice_n_dice(config: AttributeDict):
    slicer = Slicer(config)
    slicer.process()
    # set pyodbc connection
    # construct sql
    # read raw data into a lightweight data structure
