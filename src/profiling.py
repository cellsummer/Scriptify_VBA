#! /usr/bin/env python3
"""visualize batch run times"""
import pandas as pd
import json


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
    _units = 0.000001
    # _units = 1

    def __init__(
        self,
    ):
        self._load_logs()
        pass

    def _load_logs(
        self,
    ):
        """load logs"""
        self.log = pd.read_csv("inputs/log2.csv")
        self.batch = pd.read_csv("inputs/batch2.csv")
        self.dlmacro = pd.read_csv("inputs/dlmacro2.csv")

        # clean the data
        # convert timestamps
        self.log["Timestamp"] = pd.to_datetime(
            self.log["Timestamp"],
        )
        # self.log['Timestamp'] = pd.to_datetime(self.log["Timestamp"], format="%x %X")
        self.dlmacro["Time"] = self.dlmacro["Time"].fillna("00:00:00")
        self.dlmacro["Time"] = pd.to_timedelta(self.dlmacro["Time"])
        min_duration = pd.Timedelta(1, unit="s")
        self.dlmacro["Time"] = self.dlmacro["Time"].apply(
            lambda x: min_duration if x < min_duration else x
        )
        self.dlmacro["ArgumentNameTmp"] = self.dlmacro["ArgumentNameTmp"].fillna("")

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
        start_code = self._start_code
        batch_ids = self.log.query("No==@start_code").BatchId
        batches = self.batch.query("Id in @batch_ids")[["Id", "Name"]]

        # add start time
        batches["BatchId"] = batches["Id"]
        batches = batches.merge(
            self.log.query("No==@start_code")[["BatchId", "Timestamp"]],
            on="BatchId",
            how="left",
        )

        # get a list of dlmacros
        all_dlmacro_ids = self.dlmacro["Id"].unique()
        batches["is_macro"] = batches.apply(
            lambda x: True if x["Id"] in all_dlmacro_ids else False, axis=1
        )

        # get a list of dl steps
        ids = batches.Id
        dls = self.dlmacro.query("Id in @ids and Act != 3")[
            ["Id", "Reference", "Act", "ArgumentNameTmp", "Time"]
        ]
        dls["Action"] = dls["Act"].map(self._macro_action_map)
        dls["Name"] = dls["Action"] + ": " + dls["ArgumentNameTmp"]
        dls.sort_values(by=["Id", "Reference"], inplace=True)

        print(batches)
        print(dls)

        return batches, dls

    def _get_start_finish_time(
        self,
    ):
        batches, dls = self._get_events()
        batch_id = []
        event_id = []
        event_name = []
        event_start = []
        event_finish = []
        start_code = self._start_code
        finish_code = self._finish_code

        for _, row in batches.iterrows():
            # batch id is the same as event id
            batch_id.append(row["Id"])
            event_id.append(row["Id"])
            event_name.append(row["Name"])

            id = row["Id"]

            start_time = self.log.query(
                "BatchId == @id and No == @start_code"
            ).Timestamp.iloc[0]
            event_start.append(start_time)

            finish_time = self.log.query(
                "BatchId == @id and No == @finish_code"
            ).Timestamp.iloc[0]
            event_finish.append(finish_time)

        curr_batch_id = 0
        time_taken = pd.Timedelta(0)

        for _, row in dls.iterrows():
            batch_id.append(row["Id"])
            event_id.append(row["Id"] + row["Reference"] / 100)
            event_name.append(row["Name"])

            if row["Id"] != curr_batch_id:
                time_taken = pd.Timedelta(0)

            id = row["Id"]
            batch_start_time = self.log.query(
                "BatchId == @id and No == @start_code"
            ).Timestamp.iloc[0]

            batch_finish_time = self.log.query(
                "BatchId == @id and No == @finish_code"
            ).Timestamp.iloc[0]

            start_time = min(
                batch_start_time + time_taken,
                batch_finish_time - pd.Timedelta(1, unit="s"),
            )

            # TODO: this is a workaround
            min_dur = 2
            event_dur = (
                pd.Timedelta(0)
                if row["Time"] < pd.Timedelta(min_dur, unit="s")
                else row["Time"]
            )

            finish_time = min(start_time + event_dur, batch_finish_time)

            time_taken += event_dur
            curr_batch_id = row["Id"]

            event_start.append(start_time)
            event_finish.append(finish_time)

        res = {
            "event_id": event_id,
            "event_name": event_name,
            "event_start": event_start,
            "event_finish": event_finish,
        }

        df_res = pd.DataFrame(data=res)
        df_res["event_name"] = df_res["event_name"].fillna("Unknown")
        df_res["duration"] = df_res["event_finish"] - df_res["event_start"]
        df_res["duration_in_seconds"] = df_res["duration"].dt.total_seconds()
        df_res = df_res.sort_values(by=["event_start"])
        df_res.to_csv("outputs/events.csv")
        print(df_res)

        return df_res

    def create_profile(self, pid=0, tid=0):
        df = self._get_start_finish_time()
        json_profile = []
        batch_start_time = df["event_start"].iloc[0]
        for _, row in df.iterrows():
            ts = int(
                (row["event_start"] - batch_start_time).total_seconds() / self._units
            )
            dur = int(row["duration_in_seconds"] / self._units)
            dur = 1 if dur == 0 else dur
            profile_row = {
                "pid": pid,
                "tid": tid,
                "ph": "X",
                "name": row["event_name"],
                "ts": ts,
                "dur": dur,
            }
            json_profile.append(profile_row)

        # print(json_profile)
        with open("outputs/profile.json", "w") as f:
            json.dump(json_profile, f)


def batch_profiling():
    batch_profile = BatchProfile()
    # res = batch_profile._get_start_finish_time()
    # print(res)
    batch_profile.create_profile()


if __name__ == "__main__":
    batch_profiling()
