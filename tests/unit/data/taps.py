import polars as pl

df_taps_raw = pl.DataFrame(
    [
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "cellphone_recharge"},
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 2, "value_prop": "point"},
            "user_id": 3708,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 3, "value_prop": "send_money"},
            "user_id": 3708,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "transport"},
            "user_id": 93963,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 1, "value_prop": "cellphone_recharge"},
            "user_id": 93963,
        },
    ]
).with_columns(pl.col("day").str.strptime(pl.Date, strict=False))

df_taps_flat_expected = pl.DataFrame(
    [
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "cellphone_recharge",
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "position": 2,
            "value_prop": "point",
            "user_id": 3708,
        },
        {
            "day": "2020-11-01",
            "position": 3,
            "value_prop": "send_money",
            "user_id": 3708,
        },
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "transport",
            "user_id": 93963,
        },
        {
            "day": "2020-11-01",
            "position": 1,
            "value_prop": "cellphone_recharge",
            "user_id": 93963,
        },
    ]
).with_columns(pl.col("day").str.strptime(pl.Date, strict=False))
