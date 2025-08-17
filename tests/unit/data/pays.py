import polars as pl

df_pays_raw = pl.DataFrame(
    {
        "pay_date": [
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
            "2020-11-01",
        ],
        "total": [
            7.04,
            37.36,
            15.84,
            26.26,
            35.35,
            20.95,
            74.48,
            31.52,
            83.76,
            93.54,
        ],
        "user_id": [
            35994,
            79066,
            19321,
            19321,
            38438,
            85939,
            14372,
            14372,
            65274,
            65274,
        ],
        "value_prop": [
            "link_cobro",
            "cellphone_recharge",
            "cellphone_recharge",
            "send_money",
            "send_money",
            "transport",
            "prepaid",
            "link_cobro",
            "transport",
            "prepaid",
        ],
    }
).with_columns(pl.col("pay_date").str.strptime(pl.Date))
