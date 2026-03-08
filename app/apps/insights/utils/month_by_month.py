from collections import OrderedDict
from datetime import date
from decimal import Decimal

from django.db import models
from django.db.models import Sum, Case, When, Value
from django.db.models.functions import Coalesce
from django.utils.formats import date_format

from apps.currencies.models import Currency
from apps.currencies.utils.convert import convert
from apps.transactions.models import Transaction


def get_month_by_month_data(group_by="categories", account_id=None):
    """
    Aggregate transaction totals by period (month/year), grouped by categories, tags, or entities.

    Args:
        group_by: One of "categories", "tags", or "entities"
        account_id: Optional account id to filter by account

    Returns:
        {
            "periods": [{"key": "2025-01", "label": "Jan 2025"}, ...],
            "items": {
                item_id: {
                    "name": "Item Name",
                    "period_totals": {
                        "2025-01": {"currencies": {...}},
                        ...
                    },
                    "total": {"currencies": {...}}
                },
                ...
            },
            "period_totals": {...},
            "grand_total": {"currencies": {...}}
        }
    """
    # Base queryset - all paid transactions, non-muted
    transactions = Transaction.objects.filter(
        is_paid=True,
        account__is_archived=False,
    ).exclude(account__currency__is_archived=True)

    if account_id:
        transactions = transactions.filter(account_id=account_id)

    # Define grouping fields based on group_by parameter
    if group_by == "tags":
        group_field = "tags"
        name_field = "tags__name"
    elif group_by == "entities":
        group_field = "entities"
        name_field = "entities__name"
    else:  # Default to categories
        group_field = "category"
        name_field = "category__name"

    # Periods with data, sorted ascending
    period_rows = list(
        transactions.values("reference_date__year", "reference_date__month")
        .distinct()
        .order_by("reference_date__year", "reference_date__month")
    )
    periods = [
        {
            "key": _get_period_key(
                period_row["reference_date__year"], period_row["reference_date__month"]
            ),
            "label": date_format(
                date(
                    period_row["reference_date__year"],
                    period_row["reference_date__month"],
                    1,
                ),
                "M Y",
                use_l10n=True,
            ),
        }
        for period_row in period_rows
    ]
    if not periods:
        return {
            "periods": [],
            "items": {},
            "period_totals": {},
            "grand_total": {"currencies": {}},
            "chart": {"labels": [], "datasets": []},
        }

    # Aggregate by group, period, and currency
    metrics = (
        transactions.values(
            group_field,
            name_field,
            "reference_date__year",
            "reference_date__month",
            "account__currency",
            "account__currency__code",
            "account__currency__name",
            "account__currency__decimal_places",
            "account__currency__prefix",
            "account__currency__suffix",
            "account__currency__exchange_currency",
        )
        .annotate(
            expense_total=Coalesce(
                Sum(
                    Case(
                        When(type=Transaction.Type.EXPENSE, then="amount"),
                        default=Value(0),
                        output_field=models.DecimalField(),
                    )
                ),
                Decimal("0"),
            ),
            income_total=Coalesce(
                Sum(
                    Case(
                        When(type=Transaction.Type.INCOME, then="amount"),
                        default=Value(0),
                        output_field=models.DecimalField(),
                    )
                ),
                Decimal("0"),
            ),
        )
        .order_by(name_field, "reference_date__year", "reference_date__month")
    )

    # Build result structure
    result = {
        "periods": periods,
        "items": OrderedDict(),
        "period_totals": {},
        "grand_total": {"currencies": {}},
    }

    # Store currency info for later use in totals
    currency_info = {}

    for metric in metrics:
        item_id = metric[group_field]
        item_name = metric[name_field]
        period_key = _get_period_key(
            metric["reference_date__year"], metric["reference_date__month"]
        )
        currency_id = metric["account__currency"]

        # Use a consistent key for None (uncategorized/untagged/no entity)
        item_key = item_id if item_id is not None else "__none__"

        if item_key not in result["items"]:
            result["items"][item_key] = {
                "name": item_name,
                "period_totals": {},
                "total": {"currencies": {}},
            }

        if period_key not in result["items"][item_key]["period_totals"]:
            result["items"][item_key]["period_totals"][period_key] = {"currencies": {}}

        # Calculate final total (income - expense)
        final_total = metric["income_total"] - metric["expense_total"]

        # Store currency info for totals calculation
        if currency_id not in currency_info:
            currency_info[currency_id] = {
                "code": metric["account__currency__code"],
                "name": metric["account__currency__name"],
                "decimal_places": metric["account__currency__decimal_places"],
                "prefix": metric["account__currency__prefix"],
                "suffix": metric["account__currency__suffix"],
                "exchange_currency_id": metric["account__currency__exchange_currency"],
            }

        currency_data = {
            "currency": {
                "code": metric["account__currency__code"],
                "name": metric["account__currency__name"],
                "decimal_places": metric["account__currency__decimal_places"],
                "prefix": metric["account__currency__prefix"],
                "suffix": metric["account__currency__suffix"],
            },
            "final_total": final_total,
            "income_total": metric["income_total"],
            "expense_total": metric["expense_total"],
        }

        # Handle currency conversion if exchange currency is set
        if metric["account__currency__exchange_currency"]:
            from_currency = Currency.objects.get(id=currency_id)
            exchange_currency = Currency.objects.get(
                id=metric["account__currency__exchange_currency"]
            )

            converted_amount, prefix, suffix, decimal_places = convert(
                amount=final_total,
                from_currency=from_currency,
                to_currency=exchange_currency,
            )

            if converted_amount is not None:
                currency_data["exchanged"] = {
                    "final_total": converted_amount,
                    "currency": {
                        "prefix": prefix,
                        "suffix": suffix,
                        "decimal_places": decimal_places,
                        "code": exchange_currency.code,
                        "name": exchange_currency.name,
                    },
                }

        result["items"][item_key]["period_totals"][period_key]["currencies"][
            currency_id
        ] = currency_data

        # Accumulate item total (across all periods for this item)
        if currency_id not in result["items"][item_key]["total"]["currencies"]:
            result["items"][item_key]["total"]["currencies"][currency_id] = {
                "currency": currency_data["currency"].copy(),
                "final_total": Decimal("0"),
            }
        result["items"][item_key]["total"]["currencies"][currency_id][
            "final_total"
        ] += final_total

        # Accumulate period total (across all items for this period)
        if period_key not in result["period_totals"]:
            result["period_totals"][period_key] = {"currencies": {}}
        if currency_id not in result["period_totals"][period_key]["currencies"]:
            result["period_totals"][period_key]["currencies"][currency_id] = {
                "currency": currency_data["currency"].copy(),
                "final_total": Decimal("0"),
            }
        result["period_totals"][period_key]["currencies"][currency_id][
            "final_total"
        ] += final_total

        # Accumulate grand total
        if currency_id not in result["grand_total"]["currencies"]:
            result["grand_total"]["currencies"][currency_id] = {
                "currency": currency_data["currency"].copy(),
                "final_total": Decimal("0"),
            }
        result["grand_total"]["currencies"][currency_id]["final_total"] += final_total

    # Add currency conversion for item totals
    for item_key, item_data in result["items"].items():
        for currency_id, total_data in item_data["total"]["currencies"].items():
            if currency_info[currency_id]["exchange_currency_id"]:
                from_currency = Currency.objects.get(id=currency_id)
                exchange_currency = Currency.objects.get(
                    id=currency_info[currency_id]["exchange_currency_id"]
                )
                converted_amount, prefix, suffix, decimal_places = convert(
                    amount=total_data["final_total"],
                    from_currency=from_currency,
                    to_currency=exchange_currency,
                )
                if converted_amount is not None:
                    total_data["exchanged"] = {
                        "final_total": converted_amount,
                        "currency": {
                            "prefix": prefix,
                            "suffix": suffix,
                            "decimal_places": decimal_places,
                            "code": exchange_currency.code,
                            "name": exchange_currency.name,
                        },
                    }

    # Add currency conversion for period totals
    for period_key, period_data in result["period_totals"].items():
        for currency_id, total_data in period_data["currencies"].items():
            if currency_info[currency_id]["exchange_currency_id"]:
                from_currency = Currency.objects.get(id=currency_id)
                exchange_currency = Currency.objects.get(
                    id=currency_info[currency_id]["exchange_currency_id"]
                )
                converted_amount, prefix, suffix, decimal_places = convert(
                    amount=total_data["final_total"],
                    from_currency=from_currency,
                    to_currency=exchange_currency,
                )
                if converted_amount is not None:
                    total_data["exchanged"] = {
                        "final_total": converted_amount,
                        "currency": {
                            "prefix": prefix,
                            "suffix": suffix,
                            "decimal_places": decimal_places,
                            "code": exchange_currency.code,
                            "name": exchange_currency.name,
                        },
                    }

    # Add currency conversion for grand total
    for currency_id, total_data in result["grand_total"]["currencies"].items():
        if currency_info[currency_id]["exchange_currency_id"]:
            from_currency = Currency.objects.get(id=currency_id)
            exchange_currency = Currency.objects.get(
                id=currency_info[currency_id]["exchange_currency_id"]
            )
            converted_amount, prefix, suffix, decimal_places = convert(
                amount=total_data["final_total"],
                from_currency=from_currency,
                to_currency=exchange_currency,
            )
            if converted_amount is not None:
                total_data["exchanged"] = {
                    "final_total": converted_amount,
                    "currency": {
                        "prefix": prefix,
                        "suffix": suffix,
                        "decimal_places": decimal_places,
                        "code": exchange_currency.code,
                        "name": exchange_currency.name,
                    },
                }

    result["chart"] = _build_period_chart_data(periods, result["period_totals"])

    return result


def _get_period_key(year, month):
    return f"{year}-{month:02d}"


def _build_period_chart_data(periods, period_totals):
    datasets_map = OrderedDict()
    period_keys = [period["key"] for period in periods]

    for period_key in period_keys:
        for currency_id, currency_data in period_totals.get(period_key, {}).get(
            "currencies", {}
        ).items():
            if currency_id not in datasets_map:
                datasets_map[currency_id] = {
                    "label": currency_data["currency"]["code"],
                    "data": [None] * len(period_keys),
                    "currency": currency_data["currency"],
                }

    for period_index, period_key in enumerate(period_keys):
        for currency_id, currency_data in period_totals.get(period_key, {}).get(
            "currencies", {}
        ).items():
            datasets_map[currency_id]["data"][period_index] = float(
                currency_data["final_total"]
            )

    return {
        "labels": [period["label"] for period in periods],
        "datasets": list(datasets_map.values()),
    }
