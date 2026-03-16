import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import numpy as np
from dotenv import load_dotenv
from faker import Faker
from tqdm import tqdm

from ingestion.upload_logs import upload_logs_to_s3


@dataclass
class Volumes:
    orders: int
    order_items: int
    products: int
    customers: int


def load_environment() -> None:
    """
    Load environment variables from .env if present.
    """
    load_dotenv()


def get_volumes(full: bool) -> Volumes:
    """
    Return record volumes depending on whether full mode is enabled.
    """
    orders = 5_000_000 if full else 500_000
    return Volumes(
        orders=orders,
        order_items=orders * 3,
        products=50_000,
        customers=200_000,
    )


def ensure_output_dir(base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)


def sample_indices(n_rows: int, fraction: float) -> np.ndarray:
    n = int(n_rows * fraction)
    if n <= 0:
        return np.array([], dtype=int)
    return np.random.choice(n_rows, size=n, replace=False)


def generate_customers_csv(path: Path, n_rows: int, faker: Faker) -> None:
    """
    Generate customers.csv with required data issues.

    Data issues:
      - 3% duplicate emails with different IDs
      - name with extra spaces or ALL CAPS for 10%
      - 2% invalid email format
      - 15% null segment
      - country name inconsistency ('US','USA','United States')
    """
    rng = np.random.default_rng()

    customer_ids = np.arange(1, n_rows + 1)

    first_names = np.array([faker.first_name() for _ in range(n_rows)])
    last_names = np.array([faker.last_name() for _ in range(n_rows)])
    names = np.char.add(np.char.add(first_names, " "), last_names)

    emails = np.array(
        [faker.unique.email() for _ in range(int(n_rows * 1.03))], dtype=object
    )
    base_email_indices = rng.choice(emails.size, size=n_rows, replace=False)
    emails = emails[base_email_indices]

    dup_email_idx = sample_indices(n_rows, 0.03)
    if dup_email_idx.size > 0:
        src_idx = rng.choice(n_rows, size=dup_email_idx.size, replace=False)
        emails[dup_email_idx] = emails[src_idx]

    name_issue_idx = sample_indices(n_rows, 0.10)
    for idx in name_issue_idx:
        if rng.random() < 0.5:
            names[idx] = f"  {names[idx]}  "
        else:
            names[idx] = str(names[idx]).upper()

    segments = np.array(
        [rng.choice(["Retail", "Wholesale", "Online", "Enterprise"]) for _ in range(n_rows)],
        dtype=object,
    )
    segment_null_idx = sample_indices(n_rows, 0.15)
    segments[segment_null_idx] = None

    country_variants = ["US", "USA", "United States"]
    countries = np.array(
        [rng.choice(country_variants) for _ in range(n_rows)],
        dtype=object,
    )

    invalid_email_idx = sample_indices(n_rows, 0.02)
    for idx in invalid_email_idx:
        emails[idx] = emails[idx].replace("@", "")[: max(1, len(emails[idx]) - 1)]

    with path.open("w", encoding="utf-8") as f:
        f.write("customer_id,name,email,segment,country\n")
        for cid, name, email, segment, country in tqdm(
            zip(customer_ids, names, emails, segments, countries),
            total=n_rows,
            desc="Generating customers.csv",
        ):
            seg_val = "" if segment is None else segment
            f.write(
                f"{cid},\"{name}\",{email},{seg_val},{country}\n"
            )


def generate_products_csv(path: Path, n_rows: int, faker: Faker) -> None:
    """
    Generate products.csv with required issues.

    Data issues:
      - 5% category typos ('Electornics','electroncs')
      - 12% null cost_price
      - 1% duplicate product_ids
    """
    rng = np.random.default_rng()

    product_ids = np.arange(1, n_rows + 1)
    names = np.array([faker.word().title() for _ in range(n_rows)], dtype=object)

    base_categories = np.array(
        [
            rng.choice(
                [
                    "Electronics",
                    "Home",
                    "Clothing",
                    "Sports",
                    "Accessories",
                    "Books",
                ]
            )
            for _ in range(n_rows)
        ],
        dtype=object,
    )

    typo_idx = sample_indices(n_rows, 0.05)
    for idx in typo_idx:
        if rng.random() < 0.5:
            base_categories[idx] = "Electornics"
        else:
            base_categories[idx] = "electroncs"

    list_prices = np.round(
        rng.uniform(5.0, 500.0, size=n_rows).astype(float), 2
    )
    cost_prices = list_prices * rng.uniform(0.4, 0.9, size=n_rows)
    cost_prices = np.round(cost_prices, 2)

    cost_null_idx = sample_indices(n_rows, 0.12)
    cost_prices = cost_prices.astype(object)
    cost_prices[cost_null_idx] = None

    dup_idx = sample_indices(n_rows, 0.01)
    if dup_idx.size > 0:
        src_idx = rng.choice(product_ids.size, size=dup_idx.size, replace=False)
        product_ids[dup_idx] = product_ids[src_idx]

    with path.open("w", encoding="utf-8") as f:
        f.write("product_id,name,category,list_price,cost_price\n")
        for pid, name, category, list_price, cost_price in tqdm(
            zip(product_ids, names, base_categories, list_prices, cost_prices),
            total=n_rows,
            desc="Generating products.csv",
        ):
            cost_str = "" if cost_price is None else f"{cost_price:.2f}"
            f.write(
                f"{pid},\"{name}\",{category},{list_price:.2f},{cost_str}\n"
            )


def generate_orders_and_items_csv(
    orders_path: Path,
    order_items_path: Path,
    orders_n: int,
    order_items_n: int,
    customers_n: int,
    products_n: int,
    faker: Faker,
) -> None:
    """
    Generate orders.csv and order_items.csv with specified data issues.

    orders.csv issues:
      - 2% duplicate order_ids
      - date_ordered mixed formats
      - status values in random case / variants
      - total_amount as string with '$' and commas
      - 8% null shipping_region
      - 0.5% future-dated orders
      - 3% orphan customer_ids not in customers table

    order_items.csv issues:
      - 1% duplicate item_ids
      - 2% unit_price as 0 or negative
      - 3% discount > 1.0
      - quantity as float for 5% of rows
      - 2% missing order_id
    """
    rng = np.random.default_rng()

    order_ids = np.arange(1, orders_n + 1)
    dup_order_idx = sample_indices(orders_n, 0.02)
    if dup_order_idx.size > 0:
        src_idx = rng.choice(order_ids.size, size=dup_order_idx.size, replace=False)
        order_ids[dup_order_idx] = order_ids[src_idx]

    base_date = datetime.now() - timedelta(days=365)
    days_offsets = rng.integers(0, 365, size=orders_n)
    dates = np.array([base_date + timedelta(days=int(d)) for d in days_offsets])

    future_idx = sample_indices(orders_n, 0.005)
    for idx in future_idx:
        dates[idx] = datetime.now() + timedelta(days=int(rng.integers(1, 60)))

    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%b %d %Y"]
    date_strings: List[str] = []
    for d in dates:
        fmt = rng.choice(date_formats)
        date_strings.append(d.strftime(fmt))

    status_variants = ["completed", "COMPLETED", "Complete", "done", "shipped", "SHIPPED"]
    statuses = np.array(
        [rng.choice(status_variants) for _ in range(orders_n)], dtype=object
    )

    shipping_regions = np.array(
        [
            rng.choice(
                [
                    "North",
                    "South",
                    "East",
                    "West",
                    "Central",
                    "International",
                ]
            )
            for _ in range(orders_n)
        ],
        dtype=object,
    )
    shipping_null_idx = sample_indices(orders_n, 0.08)
    shipping_regions[shipping_null_idx] = None

    valid_customer_ids = np.arange(1, customers_n + 1)
    customer_ids = rng.choice(valid_customer_ids, size=orders_n, replace=True)

    orphan_idx = sample_indices(orders_n, 0.03)
    if orphan_idx.size > 0:
        orphan_ids = customers_n + 1 + rng.integers(
            1, customers_n, size=orphan_idx.size
        )
        customer_ids[orphan_idx] = orphan_ids

    total_amounts = rng.uniform(20.0, 1000.0, size=orders_n)
    total_amount_strings = [
        f"${amt:,.2f}" for amt in total_amounts
    ]

    item_ids = np.arange(1, order_items_n + 1)
    dup_item_idx = sample_indices(order_items_n, 0.01)
    if dup_item_idx.size > 0:
        src_idx = rng.choice(item_ids.size, size=dup_item_idx.size, replace=False)
        item_ids[dup_item_idx] = item_ids[src_idx]

    order_ids_for_items = rng.choice(order_ids, size=order_items_n, replace=True)
    missing_order_idx = sample_indices(order_items_n, 0.02)
    order_ids_for_items = order_ids_for_items.astype(object)
    order_ids_for_items[missing_order_idx] = None

    product_ids_for_items = rng.integers(1, products_n + 1, size=order_items_n)
    unit_prices = rng.uniform(1.0, 500.0, size=order_items_n)

    bad_price_idx = sample_indices(order_items_n, 0.02)
    for idx in bad_price_idx:
        if rng.random() < 0.5:
            unit_prices[idx] = 0.0
        else:
            unit_prices[idx] = -abs(unit_prices[idx])

    discounts = rng.uniform(0.0, 0.5, size=order_items_n)
    bad_discount_idx = sample_indices(order_items_n, 0.03)
    discounts[bad_discount_idx] = rng.uniform(1.01, 2.0, size=bad_discount_idx.size)

    quantities = rng.integers(1, 10, size=order_items_n).astype(float)
    float_qty_idx = sample_indices(order_items_n, 0.05)
    quantities[float_qty_idx] = quantities[float_qty_idx] + rng.uniform(
        0.1, 0.9, size=float_qty_idx.size
    )

    with orders_path.open("w", encoding="utf-8") as f_orders:
        f_orders.write(
            "order_id,customer_id,date_ordered,status,total_amount,shipping_region\n"
        )
        for oid, cid, date_str, status, amount_str, region in tqdm(
            zip(order_ids, customer_ids, date_strings, statuses, total_amount_strings, shipping_regions),
            total=orders_n,
            desc="Generating orders.csv",
        ):
            region_val = "" if region is None else region
            f_orders.write(
                f"{oid},{cid},{date_str},{status},{amount_str},{region_val}\n"
            )

    with order_items_path.open("w", encoding="utf-8") as f_items:
        f_items.write(
            "item_id,order_id,product_id,quantity,unit_price,discount\n"
        )
        for iid, oid, pid, qty, price, disc in tqdm(
            zip(
                item_ids,
                order_ids_for_items,
                product_ids_for_items,
                quantities,
                unit_prices,
                discounts,
            ),
            total=order_items_n,
            desc="Generating order_items.csv",
        ):
            order_val = "" if oid is None else oid
            f_items.write(
                f"{iid},{order_val},{pid},{qty},{price:.2f},{disc:.4f}\n"
            )


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic ecommerce CSV data with embedded data quality issues."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Generate full volume (5,000,000 orders) instead of test volume (500,000 orders).",
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Output directory for generated CSV files (default: data/generated).",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    load_environment()

    args = parse_args(argv)
    volumes = get_volumes(full=args.full)

    print("Starting synthetic data generation...")
    print(f"  Mode        : {'FULL' if args.full else 'TEST'}")
    print(f"  Output dir  : {Path(args.output_dir).resolve()}")
    print(
        "  Volumes     : "
        f"orders={volumes.orders:,}, "
        f"order_items={volumes.order_items:,}, "
        f"products={volumes.products:,}, "
        f"customers={volumes.customers:,}"
    )

    base_output = Path(args.output_dir)
    ensure_output_dir(base_output)

    faker = Faker()

    customers_path = base_output / "customers.csv"
    products_path = base_output / "products.csv"
    orders_path = base_output / "orders.csv"
    order_items_path = base_output / "order_items.csv"

    print("\n[1/3] Generating customers.csv...")
    generate_customers_csv(customers_path, volumes.customers, faker)
    print("Completed customers.csv")

    print("\n[2/3] Generating products.csv...")
    generate_products_csv(products_path, volumes.products, faker)
    print("Completed products.csv")

    print("\n[3/3] Generating orders.csv and order_items.csv...")
    generate_orders_and_items_csv(
        orders_path=orders_path,
        order_items_path=order_items_path,
        orders_n=volumes.orders,
        order_items_n=volumes.order_items,
        customers_n=volumes.customers,
        products_n=volumes.products,
        faker=faker,
    )
    print("Completed orders.csv and order_items.csv")
    print("\nData generation finished.")

    # Upload any logs generated by ingestion scripts to S3.
    # This relies on environment configuration and .env settings.
    try:
        upload_logs_to_s3()
    except Exception as exc:  # pragma: no cover - best-effort log upload
        print(f"Warning: failed to upload ingestion logs to S3: {exc}")


if __name__ == "__main__":
    main()

