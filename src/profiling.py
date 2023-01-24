#! /usr/bin/env python3
"""visualize batch run times"""
import pandas as pd


class BatchProfile:
    """class to deal with batch run time"""

    _start_code = 8169
    _finish_code = 7473
    _macro_action_map = {
        0: "Initialize",
        1: "Load",
        2: "Map",
        3: "Run batch",
        4: "Run query",
        5: "Export seriatim",
        6: "Update assumptions",
        9: "Build risk classes",
        10: "Remove cells",
    }

    def __init__(
        self,
    ):
        self._load_logs()
        pass

    def _load_logs(
        self,
    ):
        """load logs"""
        self.log = pd.read_csv("temp/log.csv")
        self.batch = pd.read_csv("temp/batch.csv")
        self.dlmacro = pd.read_csv("temp/dlmacro.csv")

        # clean the data
        # convert timestamps
        self.log['Timestamp'] = pd.to_datetime(self.log["Timestamp"], format="%m/%d/%y %H:%M")
        self.dlmacro["Time"] = pd.to_timedelta(self.dlmacro["Time"])
        self.dlmacro["ArgumentNameTmp"] = self.dlmacro["ArgumentNameTmp"].fillna("")

        print(self.log.head())
        return

    def _create_frames(
        self,
    ):
        pass

    def _get_events(
        self,
    ):
        """get a list of events(batch + datalink step)"""
        # get a list of running batches from log
        filt = self.log["No"] == self._start_code
        batch_ids = self.log.loc[filt, "BatchId"]
        filt = self.batch["Id"].isin(batch_ids)
        batches = self.batch.loc[filt, ["Id", "Name"]]

        # add start time
        filt = self.log["No"] == self._start_code
        batches["BatchId"] = batches["Id"]
        batches = batches.merge(
            self.log.loc[filt, ["BatchId", "Timestamp"]], on="BatchId", how="left"
        )

        # get a list of dlmacros
        all_dlmacro_ids = self.dlmacro["Id"].unique()
        batches["is_macro"] = batches.apply(
            lambda x: True if x["Id"] in all_dlmacro_ids else False, axis=1
        )

        # get a list of dl steps
        filt = self.dlmacro["Id"].isin(batches["Id"])
        dls = self.dlmacro.loc[
            filt, ["Id", "Reference", "Act", "ArgumentNameTmp", "Time"]
        ]
        dls["Action"] = dls["Act"].map(self._macro_action_map)
        dls["Name"] = dls["Action"] + ": " + dls["ArgumentNameTmp"]

        dls.sort_values(by=["Id", "Reference"], inplace=True)

        print(type((self.log['Timestamp'][0])))
        print(batches)
        print(type(batches['Timestamp'][0]))
        # print(dls)
        # print(batches)

        return batches, dls

    def _get_start_finish_time(
        self,
    ):
        batches, dls = self._get_events()
        event_id = []
        event_name = []
        event_start = []
        event_finish = []

        for _, row in batches.iterrows():
            event_id.append(row["Id"])
            event_name.append(row["Name"])

            id = row["Id"]

            filt = (self.log["BatchId"] == id) & (self.log["No"] == self._start_code)
            start_time = self.log.loc[filt, 'Timestamp'].values[0]
            event_start.append(start_time)

            filt = (self.log["BatchId"] == id) & (self.log["No"] == self._finish_code)
            finish_time = self.log.loc[filt, 'Timestamp'].values[0]
            event_finish.append(finish_time)

        curr_batch_id = 0
        time_taken = pd.Timedelta(0)
        for _, row in dls.iterrows():
            event_id.append(row["Id"] + row["Reference"] / 100)
            event_name.append(row["Name"])

            if row["Id"] != curr_batch_id:
                time_taken = pd.Timedelta(0)

            batch_start_time = batches.loc[
                batches["Id"] == row["Id"], "Timestamp"
            ].values[0]


            start_time = batch_start_time + time_taken
            finish_time = start_time + row["Time"]

            time_taken += row["Time"]
            curr_batch_id = row["Id"]

            event_start.append(start_time)
            event_finish.append(finish_time)

        res = {
            "event_id": event_id,
            "event_name": event_name,
            "event_start": event_start,
            "event_finish": event_finish,
        }

        return res

    def create_profile(
        self,
    ):
        pass

    def visualize_profile(
        self,
    ):
        pass


def batch_profiling():
    batch_profile = BatchProfile()
    res = batch_profile._get_start_finish_time()
    print(pd.DataFrame(data=res))


if __name__ == "__main__":
    batch_profiling()
