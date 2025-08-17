import polars as pl

df_prints_raw = pl.DataFrame(
    [
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "cellphone_recharge"},
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 1, "value_prop": "prepaid"},
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "prepaid"},
            "user_id": 63252,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "cellphone_recharge"},
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 1, "value_prop": "link_cobro"},
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 2, "value_prop": "credits_consumer"},
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 3, "value_prop": "point"},
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "point"},
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 1, "value_prop": "credits_consumer"},
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 2, "value_prop": "transport"},
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "event_data": {"position": 0, "value_prop": "point"},
            "user_id": 57587,
        },
    ]
).with_columns(pl.col("day").str.strptime(pl.Date, strict=False))

df_prints_flat_expected = pl.DataFrame(
    [
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "cellphone_recharge",
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "position": 1,
            "value_prop": "prepaid",
            "user_id": 98702,
        },
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "prepaid",
            "user_id": 63252,
        },
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "cellphone_recharge",
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "position": 1,
            "value_prop": "link_cobro",
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "position": 2,
            "value_prop": "credits_consumer",
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "position": 3,
            "value_prop": "point",
            "user_id": 24728,
        },
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "point",
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "position": 1,
            "value_prop": "credits_consumer",
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "position": 2,
            "value_prop": "transport",
            "user_id": 25517,
        },
        {
            "day": "2020-11-01",
            "position": 0,
            "value_prop": "point",
            "user_id": 57587,
        },
    ]
).with_columns(pl.col("day").str.strptime(pl.Date, strict=False))
