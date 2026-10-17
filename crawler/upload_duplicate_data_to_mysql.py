from sqlalchemy import text

from crawler.upload_data_to_mysql import create_db_engine


CHECKS = {
    "housing_price_index": {
        "keys": ["period", "region"],
        "values": ["housing_price_index"],
    },
    "cpi": {
        "keys": ["period"],
        "values": ["cpi_index"],
    },
    "hpi": {
        "keys": ["period", "region"],
        "values": [
            "housing_price_index",
            "cpi_index",
            "cpi_2016_base100",
            "real_hpi",
        ],
    },
}


def main():
    engine = create_db_engine()

    try:
        with engine.connect() as conn:
            for table, spec in CHECKS.items():
                keys = spec["keys"]
                values = spec["values"]
                columns = keys + values

                select_columns = ", ".join(
                    f"`{column}`" for column in columns
                )
                group_columns = ", ".join(
                    f"`{column}`" for column in columns
                )

                query = text(
                    f"""
                    SELECT COUNT(*) AS duplicate_groups
                    FROM (
                        SELECT {group_columns}
                        FROM `{table}`
                        GROUP BY {group_columns}
                        HAVING COUNT(*) > 1
                    ) AS duplicates
                    """
                )

                duplicate_groups = conn.execute(query).scalar_one()

                print(f"\n--- {table} ---")
                print(
                    "Groups with identical key and values:",
                    duplicate_groups,
                )

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()