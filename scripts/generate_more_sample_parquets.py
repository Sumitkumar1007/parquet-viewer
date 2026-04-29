from __future__ import annotations

from pathlib import Path

import duckdb


OUTPUT_DIR = Path("data")


def write_parquet(filename: str, sql: str) -> None:
    target = str((OUTPUT_DIR / filename).resolve()).replace("'", "''")
    with duckdb.connect(database=":memory:") as conn:
        conn.execute(f"COPY ({sql}) TO '{target}' (FORMAT PARQUET)")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_parquet(
        "sales_q1.parquet",
        """
        SELECT * FROM (
            VALUES
                (1, 'North', 'Enterprise', 125000.50, 44, DATE '2026-01-05'),
                (2, 'South', 'SMB', 77500.00, 28, DATE '2026-01-12'),
                (3, 'West', 'Consumer', 49999.99, 76, DATE '2026-02-03'),
                (4, 'East', 'Enterprise', 166320.12, 53, DATE '2026-02-18')
        ) AS t(id, region, category, revenue, units_sold, order_date)
        """,
    )

    write_parquet(
        "inventory_snapshot.parquet",
        """
        SELECT * FROM (
            VALUES
                ('SKU-100', 'Keyboard', 'Accessories', 320, 'A1'),
                ('SKU-205', 'Monitor', 'Displays', 86, 'B4'),
                ('SKU-411', 'Dock', 'Accessories', 144, 'C2'),
                ('SKU-620', 'Laptop', 'Computers', 39, 'D7')
        ) AS t(sku, product_name, category, stock_on_hand, warehouse_bin)
        """,
    )

    write_parquet(
        "customer_support.parquet",
        """
        SELECT * FROM (
            VALUES
                (1001, 'open', 'billing', 'high', DATE '2026-04-01'),
                (1002, 'closed', 'technical', 'medium', DATE '2026-04-02'),
                (1003, 'pending', 'shipping', 'low', DATE '2026-04-03'),
                (1004, 'open', 'technical', 'high', DATE '2026-04-04')
        ) AS t(ticket_id, status, topic, priority, created_date)
        """,
    )

    print("Wrote sample parquet files:")
    for path in sorted(OUTPUT_DIR.glob("*.parquet")):
        print(path.resolve())


if __name__ == "__main__":
    main()
