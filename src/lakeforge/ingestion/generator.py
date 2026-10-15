"""Deterministic synthetic marketplace data.

Every batch is derived only from ``(seed, day_index)``, so regenerating a single day yields
byte-identical files. The data is fictional and intentionally contains defects
(duplicates, orphans, malformed records, inconsistent casing) so that the Silver layer has
something real to clean.
"""

from __future__ import annotations

import csv
import json
import logging
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from lakeforge.schemas import CUSTOMER_FIELDS, PRODUCT_FIELDS, TRANSACTION_FIELDS

logger = logging.getLogger(__name__)

DEFAULT_SEED = 42
DEFAULT_START_DATE = date(2026, 1, 5)

INITIAL_CUSTOMERS = 120
NEW_CUSTOMERS_PER_DAY = 8
INITIAL_PRODUCTS = 40

FIRST_NAMES = (
    "Ada", "Liam", "Maya", "Noah", "Elif", "Omar", "Sara", "Jonas", "Lena", "Mateo",
    "Yuki", "Aria", "Kerem", "Nora", "Ivan", "Zoe", "Emre", "Ines", "Theo", "Mila",
)  # fmt: skip
LAST_NAMES = (
    "Silva", "Novak", "Kaya", "Fischer", "Rossi", "Haddad", "Tanaka", "Moreau", "Larsen",
    "Okafor", "Petrov", "Garcia", "Yilmaz", "Becker", "Costa", "Nguyen",
)  # fmt: skip
COUNTRIES = ("US", "GB", "DE", "FR", "NL", "TR", "ES", "IT", "SE", "PL")
CATEGORIES = {
    "electronics": ("Wireless Earbuds", "USB-C Hub", "Smart Plug", "Webcam", "Power Bank"),
    "home": ("Desk Lamp", "Ceramic Mug", "Throw Blanket", "Wall Clock", "Plant Pot"),
    "books": ("Data Systems Handbook", "SQL Pocket Guide", "Mystery Novel", "Cookbook"),
    "sports": ("Yoga Mat", "Water Bottle", "Resistance Bands", "Running Socks"),
    "beauty": ("Face Serum", "Lip Balm", "Shampoo Bar", "Hand Cream"),
}
ORDER_STATUSES = ("placed", "shipped", "delivered", "cancelled")
ORDER_STATUS_WEIGHTS = (35, 30, 27, 8)
STATUS_PROGRESSION = {"placed": ("shipped", "cancelled"), "shipped": ("delivered",)}
PAYMENT_METHODS = ("card", "paypal", "bank_transfer", "gift_card")
DISCOUNTS = ("0.00", "0.00", "0.00", "0.05", "0.10", "0.15")
CENT = Decimal("0.01")


@dataclass
class Batch:
    batch_date: date
    customers: list[dict[str, str | None]] = field(default_factory=list)
    products: list[dict[str, str | None]] = field(default_factory=list)
    orders: list[dict] = field(default_factory=list)
    transactions: list[dict[str, str | None]] = field(default_factory=list)
    malformed_order_lines: list[str] = field(default_factory=list)


def _rng(seed: int, day_index: int, stream: str) -> random.Random:
    return random.Random(f"{seed}:{day_index}:{stream}")


def _ts(day: date, rng: random.Random, earliest_hour: int = 0) -> str:
    seconds = rng.randint(earliest_hour * 3600, 86_399)
    moment = datetime.combine(day, time()) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def customer_count(day_index: int) -> int:
    return INITIAL_CUSTOMERS + NEW_CUSTOMERS_PER_DAY * day_index


def product_count(day_index: int) -> int:
    return INITIAL_PRODUCTS + day_index // 2


def signup_day(number: int) -> int:
    if number <= INITIAL_CUSTOMERS:
        return 0
    return (number - INITIAL_CUSTOMERS - 1) // NEW_CUSTOMERS_PER_DAY + 1


def customer_id(number: int) -> str:
    return f"C{number:06d}"


def product_id(number: int) -> str:
    return f"P{number:04d}"


def _product_catalog() -> list[tuple[str, str]]:
    """Interleave categories so that any prefix of the catalog covers all of them."""
    catalog = []
    longest = max(len(names) for names in CATEGORIES.values())
    for edition in ("Standard", "Plus", "Pro"):
        for position in range(longest):
            for category, names in CATEGORIES.items():
                if position < len(names):
                    catalog.append((category, f"{names[position]} {edition}"))
    return catalog


def base_price(seed: int, number: int) -> Decimal:
    rng = random.Random(f"{seed}:price:{number}")
    return Decimal(rng.randint(499, 19_999)) / 100


def current_price(seed: int, number: int, day_index: int) -> Decimal:
    """Price after all repricing events up to and including ``day_index``."""
    price = base_price(seed, number)
    for day in range(1, day_index + 1):
        for repriced, factor in _repricing_events(seed, day):
            if repriced == number:
                price = (price * factor).quantize(CENT, ROUND_HALF_UP)
    return price


def _repricing_events(seed: int, day_index: int) -> list[tuple[int, Decimal]]:
    rng = _rng(seed, day_index, "repricing")
    numbers = rng.sample(range(1, product_count(day_index - 1) + 1), 2)
    return [(number, Decimal(rng.choice(("0.90", "0.95", "1.05", "1.10")))) for number in numbers]


def _customer_row(seed: int, number: int, day: date, rng: random.Random) -> dict[str, str | None]:
    profile = random.Random(f"{seed}:customer:{number}")
    first = profile.choice(FIRST_NAMES)
    last = profile.choice(LAST_NAMES)
    return {
        "customer_id": customer_id(number),
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}{number}@example.com",
        "country_code": profile.choice(COUNTRIES),
        "signup_date": day.isoformat(),
        "updated_at": _ts(day, rng),
    }


def _customers(seed: int, day_index: int, day: date) -> list[dict[str, str | None]]:
    rng = _rng(seed, day_index, "customers")
    first_new = 1 if day_index == 0 else customer_count(day_index - 1) + 1
    rows = [
        _customer_row(seed, n, day, rng) for n in range(first_new, customer_count(day_index) + 1)
    ]

    if day_index > 0:
        start_date = day - timedelta(days=day_index)
        for number in rng.sample(range(1, first_new), 3):
            row = _customer_row(seed, number, start_date + timedelta(days=signup_day(number)), rng)
            row["country_code"] = rng.choice(COUNTRIES)
            row["email"] = f"{row['first_name']}.{row['last_name']}.{number}@mail.example.org"
            row["updated_at"] = _ts(day, rng, earliest_hour=12)
            rows.append(row)

    rows[0]["email"] = f"  {str(rows[0]['email']).upper()} "
    rows[1]["country_code"] = str(rows[1]["country_code"]).lower()
    rows[2]["email"] = None
    rows.append(dict(rows[3]))
    if day_index == 0:
        rows.append({**rows[4], "customer_id": None})
    return rows


def _products(seed: int, day_index: int, day: date) -> list[dict[str, str | None]]:
    rng = _rng(seed, day_index, "products")
    catalog = _product_catalog()
    first_new = 1 if day_index == 0 else product_count(day_index - 1) + 1
    numbers = list(range(first_new, product_count(day_index) + 1))
    numbers += [number for number, _ in _repricing_events(seed, day_index)] if day_index else []

    rows: list[dict[str, str | None]] = []
    for number in numbers:
        category, name = catalog[(number - 1) % len(catalog)]
        rows.append(
            {
                "product_id": product_id(number),
                "product_name": name,
                "category": category,
                "unit_price": str(current_price(seed, number, day_index)),
                "is_active": "true",
                "updated_at": _ts(day, rng),
            }
        )

    if day_index == 0:
        rows[5]["category"] = None
        rows[6]["is_active"] = "FALSE"
        rows.append({**rows[7], "product_id": product_id(9999), "unit_price": "not-a-price"})
    return rows


def _order_lines(seed: int, day_index: int, rng: random.Random) -> list[dict[str, str]]:
    products = rng.sample(range(1, product_count(day_index) + 1), rng.randint(1, 4))
    return [
        {
            "product_id": product_id(number),
            "quantity": str(rng.randint(1, 5)),
            "unit_price": str(current_price(seed, number, day_index)),
            "discount_pct": rng.choice(DISCOUNTS),
        }
        for number in products
    ]


def order_total(order: dict) -> Decimal:
    total = Decimal(0)
    for item in order["items"]:
        gross = Decimal(item["unit_price"]) * int(item["quantity"])
        total += (gross * (1 - Decimal(item["discount_pct"]))).quantize(CENT, ROUND_HALF_UP)
    return total


def _base_orders(seed: int, day_index: int, day: date) -> list[dict]:
    rng = _rng(seed, day_index, "orders")
    orders = []
    for sequence in range(1, 61 + rng.randint(0, 20)):
        late = day_index > 0 and rng.random() < 0.08
        order_day = day - timedelta(days=1) if late else day
        order_ts = _ts(order_day, rng)
        orders.append(
            {
                "order_id": f"O{day:%Y%m%d}{sequence:04d}",
                "customer_id": customer_id(rng.randint(1, customer_count(day_index))),
                "order_ts": order_ts,
                "status": rng.choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)[0],
                "updated_at": order_ts,
                "items": _order_lines(seed, day_index, rng),
            }
        )
    return orders


def _status_updates(seed: int, day_index: int, day: date) -> list[dict]:
    if day_index == 0:
        return []
    rng = _rng(seed, day_index, "status-updates")
    previous = _base_orders(seed, day_index - 1, day - timedelta(days=1))
    candidates = [order for order in previous if order["status"] in STATUS_PROGRESSION]
    updates = []
    for order in rng.sample(candidates, min(8, len(candidates))):
        updates.append(
            {
                **order,
                "status": rng.choice(STATUS_PROGRESSION[order["status"]]),
                "updated_at": _ts(day, rng, earliest_hour=6),
            }
        )
    return updates


def _transactions(
    orders: list[dict], updates: list[dict], day: date, rng: random.Random
) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []

    def add(order: dict, status: str, amount: Decimal, ts: str) -> None:
        rows.append(
            {
                "transaction_id": f"T{day:%Y%m%d}{len(rows):05d}",
                "order_id": order["order_id"],
                "transaction_ts": ts,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "amount": str(amount),
                "currency": "USD",
                "status": status,
            }
        )

    for order in orders:
        if order["status"] == "cancelled":
            continue
        if rng.random() < 0.05:
            add(order, "failed", order_total(order), order["order_ts"])
        add(order, "captured", order_total(order), order["order_ts"])

    for order in updates:
        if order["status"] == "cancelled":
            add(order, "refunded", order_total(order), order["updated_at"])

    rows[0]["currency"] = "usd"
    rows[1]["amount"] = "N/A"
    rows.append(dict(rows[2]))
    rows.append({**rows[3], "transaction_id": f"T{day:%Y%m%d}99999", "order_id": "O000000000000"})
    return rows


def build_batch(seed: int, day_index: int, start_date: date = DEFAULT_START_DATE) -> Batch:
    day = start_date + timedelta(days=day_index)
    orders = _base_orders(seed, day_index, day)
    updates = _status_updates(seed, day_index, day)
    transactions = _transactions(orders, updates, day, _rng(seed, day_index, "transactions"))

    defects = [dict(order) for order in orders[:5]]
    defects[0]["order_id"] = f"O{day:%Y%m%d}9001"
    defects[0]["customer_id"] = "C999999"
    defects[1]["order_id"] = f"O{day:%Y%m%d}9002"
    defects[1]["customer_id"] = None
    defects[2]["order_id"] = f"O{day:%Y%m%d}9003"
    defects[2]["items"] = [{**defects[2]["items"][0], "quantity": "0"}]
    defects[3]["order_id"] = f"O{day:%Y%m%d}9004"
    defects[3]["order_ts"] = "2026-13-45T25:00:00Z"
    duplicates = [dict(orders[10]), dict(orders[11])]

    malformed = json.dumps(orders[12])[:-25]

    return Batch(
        batch_date=day,
        customers=_customers(seed, day_index, day),
        products=_products(seed, day_index, day),
        orders=orders + updates + defects[:4] + duplicates,
        transactions=transactions,
        malformed_order_lines=[malformed],
    )


def _write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, str | None]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_batch(batch: Batch, landing_dir: Path) -> dict[str, Path]:
    partition = f"batch_date={batch.batch_date.isoformat()}"
    paths = {}
    for entity in ("customers", "products", "orders", "transactions"):
        directory = landing_dir / entity / partition
        directory.mkdir(parents=True, exist_ok=True)
        suffix = "jsonl" if entity == "orders" else "csv"
        paths[entity] = directory / f"{entity}_{batch.batch_date:%Y%m%d}.{suffix}"

    _write_csv(paths["customers"], CUSTOMER_FIELDS, batch.customers)
    _write_csv(paths["products"], PRODUCT_FIELDS, batch.products)
    _write_csv(paths["transactions"], TRANSACTION_FIELDS, batch.transactions)
    with paths["orders"].open("w", encoding="utf-8") as handle:
        for order in batch.orders:
            handle.write(json.dumps(order, sort_keys=True) + "\n")
        for line in batch.malformed_order_lines:
            handle.write(line + "\n")
    return paths


def generate(
    landing_dir: Path,
    days: int,
    seed: int = DEFAULT_SEED,
    start_date: date = DEFAULT_START_DATE,
    first_day_index: int = 0,
) -> list[date]:
    if days < 1:
        raise ValueError("days must be at least 1")
    if first_day_index < 0:
        raise ValueError("first_day_index must not be negative")

    written = []
    for day_index in range(first_day_index, first_day_index + days):
        batch = build_batch(seed, day_index, start_date)
        write_batch(batch, landing_dir)
        written.append(batch.batch_date)
        logger.info(
            "Generated batch %s: %d customers, %d products, %d orders, %d transactions",
            batch.batch_date,
            len(batch.customers),
            len(batch.products),
            len(batch.orders) + len(batch.malformed_order_lines),
            len(batch.transactions),
        )
    return written
