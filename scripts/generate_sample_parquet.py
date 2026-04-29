from __future__ import annotations

from pathlib import Path

import duckdb


OUTPUT_DIR = Path("data")
OUTPUT_PATH = OUTPUT_DIR / "sample.parquet"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    target = str(OUTPUT_PATH).replace("'", "''")
    with duckdb.connect(database=":memory:") as conn:
        conn.execute(
            f"""
            COPY (
                SELECT
                    row_number() OVER () AS id,
                    region,
                    category,
                    revenue,
                    units_sold,
                    order_date
                FROM (
                    VALUES
                        ('North', 'Enterprise', 125000.50, 44, DATE '2026-01-05'),
                        ('South', 'SMB', 77500.00, 28, DATE '2026-01-12'),
                        ('West', 'Consumer', 49999.99, 76, DATE '2026-02-03'),
                        ('East', 'Enterprise', 166320.12, 53, DATE '2026-02-18'),
                        ('North', 'Consumer', 38640.10, 82, DATE '2026-03-02'),
                        ('West', 'SMB', 89320.76, 31, DATE '2026-03-11'),
                        ('East', 'Consumer', 58750.45, 67, DATE '2026-03-24')
                ) AS t(region, category, revenue, units_sold, order_date)
            ) TO '{target}' (FORMAT PARQUET)
            """
        )

    print(f"Wrote sample parquet to {OUTPUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
