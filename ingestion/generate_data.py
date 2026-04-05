import argparse
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
from dotenv import load_dotenv
from faker import Faker
from tqdm import tqdm

# When using --run-id N, surrogate keys start at N * ID_RUN_STRIDE + 1 per table.
# Must exceed the largest single-table row count in full mode (15M order_items).
ID_RUN_STRIDE = 25_000_000


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


def resolve_id_base(args: argparse.Namespace) -> int:
    """
    Choose the integer offset for all surrogate keys.

    Priority: --id-base > --run-id > GENERATE_DATA_ID_BASE env > 0.

    With --run-id K, keys start at K * ID_RUN_STRIDE + 1 (per entity range),
    so successive runs do not reuse the same order_id / item_id / etc.
    """
    if args.id_base is not None:
        return int(args.id_base)
    if args.run_id is not None:
        return int(args.run_id) * ID_RUN_STRIDE
    env_val = os.environ.get("GENERATE_DATA_ID_BASE")
    if env_val is not None and str(env_val).strip() != "":
        return int(str(env_val).strip())
    return 0


def sample_indices(n_rows: int, fraction: float) -> np.ndarray:
    n = int(n_rows * fraction)
    if n <= 0:
        return np.array([], dtype=int)
    return np.random.choice(n_rows, size=n, replace=False)


def generate_customers_csv(
    path: Path,
    n_rows: int,
    faker: Faker,
    issue_rate: float,
    id_base: int = 0,
) -> None:
    """
    Generate customers.csv.

    The only quarantine-triggering issue is:
      - `issue_rate` fraction of rows with invalid email format
    """
    rng = np.random.default_rng()

    customer_ids = np.arange(id_base + 1, id_base + n_rows + 1)

    first_names = np.array([faker.first_name() for _ in range(n_rows)])
    last_names = np.array([faker.last_name() for _ in range(n_rows)])
    names = np.char.add(np.char.add(first_names, " "), last_names)

    emails = np.array([faker.unique.email() for _ in range(n_rows)], dtype=object)

    segments = np.array(
        [rng.choice(["Retail", "Wholesale", "Online", "Enterprise"]) for _ in range(n_rows)],
        dtype=object,
    )
    countries = np.array(["United States" for _ in range(n_rows)], dtype=object)

    invalid_email_idx = sample_indices(n_rows, issue_rate)
    for idx in invalid_email_idx:
        # Ensure the email fails the simple regex used in `customers_transform.py`.
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


def generate_products_csv(
    path: Path,
    n_rows: int,
    faker: Faker,
    issue_rate: float,
    id_base: int = 0,
) -> None:
    """
    Generate products.csv.

    The only quarantine-triggering issue is:
      - `issue_rate` fraction of rows with non-positive cost_price
    """
    rng = np.random.default_rng()

    product_ids = np.arange(id_base + 1, id_base + n_rows + 1)
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

    list_prices = np.round(
        rng.uniform(5.0, 500.0, size=n_rows).astype(float), 2
    )
    cost_prices = np.round(list_prices * rng.uniform(0.4, 0.9, size=n_rows), 2)
    # Avoid triggering `price_below_cost_price` just from rounding errors.
    cost_prices = np.minimum(cost_prices, list_prices - 0.01)

    invalid_product_idx = sample_indices(n_rows, issue_rate)
    for idx in invalid_product_idx:
        cost_prices[idx] = 0.0  # Triggers `invalid_cost_price` in products_transform.py

    with path.open("w", encoding="utf-8") as f:
        f.write("product_id,name,category,list_price,cost_price\n")
        for pid, name, category, list_price, cost_price in tqdm(
            zip(product_ids, names, base_categories, list_prices, cost_prices),
            total=n_rows,
            desc="Generating products.csv",
        ):
            f.write(
                f"{pid},\"{name}\",{category},{list_price:.2f},{cost_price:.2f}\n"
            )


def generate_orders_and_items_csv(
    orders_path: Path,
    order_items_path: Path,
    orders_n: int,
    order_items_n: int,
    customers_n: int,
    products_n: int,
    faker: Faker,
    issue_rate: float,
    id_base: int = 0,
) -> None:
    """
    Generate orders.csv and order_items.csv.

    Quarantine should be driven primarily by:
      - `issue_rate` fraction of orders referencing missing customers
    Order_items remain "field-clean" and will be quarantined only due to missing
    foreign-key references from quarantined orders/products.
    """
    rng = np.random.default_rng()

    order_ids = np.arange(id_base + 1, id_base + orders_n + 1)
    base_date = datetime.now() - timedelta(days=365)
    days_offsets = rng.integers(0, 365, size=orders_n)
    dates = np.array([base_date + timedelta(days=int(d)) for d in days_offsets])

    # Use a single unambiguous date format so Athena/Spark parsing is stable.
    date_strings = [d.strftime("%Y-%m-%d") for d in dates]

    status_variants = ["completed", "shipped", "done"]
    statuses = np.array([rng.choice(status_variants) for _ in range(orders_n)], dtype=object)

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

    valid_customer_ids = np.arange(id_base + 1, id_base + customers_n + 1)
    customer_ids = rng.choice(valid_customer_ids, size=orders_n, replace=True)

    bad_orders_idx = sample_indices(orders_n, issue_rate)
    for idx in bad_orders_idx:
        customer_ids[idx] = id_base + customers_n + 1 + int(
            rng.integers(1, max(2, customers_n))
        )

    # Keep total_amount parse-safe: no thousand separators => no CSV column-shifting surprises.
    total_amounts = rng.uniform(20.0, 500.0, size=orders_n)
    total_amount_strings = [f"${amt:.2f}" for amt in total_amounts]

    item_ids = np.arange(id_base + 1, id_base + order_items_n + 1)
    # Keep a stable 1-to-N mapping between orders and items (e.g., 3 items/order in this project).
    items_per_order = order_items_n // orders_n if orders_n else 0
    if orders_n > 0 and items_per_order * orders_n == order_items_n:
        order_ids_for_items = np.repeat(order_ids, items_per_order)
    else:
        order_ids_for_items = rng.choice(order_ids, size=order_items_n, replace=True)

    product_ids_for_items = rng.integers(
        id_base + 1, id_base + products_n + 1, size=order_items_n
    )
    unit_prices = rng.uniform(1.0, 500.0, size=order_items_n)

    discounts = rng.uniform(0.0, 0.5, size=order_items_n)

    quantities = rng.integers(1, 10, size=order_items_n)

    with orders_path.open("w", encoding="utf-8") as f_orders:
        f_orders.write(
            "order_id,customer_id,date_ordered,status,total_amount,shipping_region\n"
        )
        for oid, cid, date_str, status, amount_str, region in tqdm(
            zip(order_ids, customer_ids, date_strings, statuses, total_amount_strings, shipping_regions),
            total=orders_n,
            desc="Generating orders.csv",
        ):
            f_orders.write(
                f"{oid},{cid},{date_str},{status},{amount_str},{region}\n"
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
            f_items.write(
                f"{iid},{oid},{pid},{qty},{price:.2f},{disc:.4f}\n"
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
    parser.add_argument(
        "--issue-records-rate",
        type=float,
        default=None,
        metavar="RATE",
        help="Fraction of rows to inject quarantine-triggering issues (0..1). "
        "If omitted, a random rate between 0%% and 5%% is chosen for this run.",
    )
    parser.add_argument(
        "--id-base",
        type=int,
        default=None,
        metavar="N",
        help="Add N to every generated surrogate key (customer_id, product_id, "
        "order_id, item_id). Use this or --run-id so each load run gets non-overlapping "
        "IDs when appending to a warehouse. Overrides --run-id.",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        default=None,
        metavar="K",
        help=f"Shorthand for --id-base K*{ID_RUN_STRIDE:,} (non-overlapping blocks per run). "
        "Ignored if --id-base is set.",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    load_environment()

    args = parse_args(argv)
    volumes = get_volumes(full=args.full)
    id_base = resolve_id_base(args)

    if args.issue_records_rate is None:
        issue_rate = random.uniform(0.0, 0.05)
        issue_rate_note = "random 0–5%"
    else:
        issue_rate = args.issue_records_rate
        issue_rate_note = "--issue-records-rate"

    print("Starting synthetic data generation...")
    print(f"  Mode        : {'FULL' if args.full else 'TEST'}")
    print(f"  Output dir  : {Path(args.output_dir).resolve()}")
    print(f"  ID base     : {id_base:,} (surrogate keys start at {id_base + 1:,} per table)")
    print(f"  Issue rate  : {issue_rate:.2%} ({issue_rate_note})")
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
    generate_customers_csv(
        customers_path,
        volumes.customers,
        faker,
        issue_rate=issue_rate,
        id_base=id_base,
    )
    print("Completed customers.csv")

    print("\n[2/3] Generating products.csv...")
    generate_products_csv(
        products_path,
        volumes.products,
        faker,
        issue_rate=issue_rate,
        id_base=id_base,
    )
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
        issue_rate=issue_rate,
        id_base=id_base,
    )
    print("Completed orders.csv and order_items.csv")
    print("\nData generation finished.")

if __name__ == "__main__":
    main()

