from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from apps.accounts.models import Account, AccountGroup
from apps.currencies.models import Currency
from apps.transactions.models import (
    Transaction,
    TransactionCategory,
    TransactionEntity,
    TransactionTag,
)


@override_settings(
    STORAGES={
        "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"
        },
    },
    WHITENOISE_AUTOREFRESH=True,
)
class InsightsYearAndMonthViewsTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            email="insights@test.com", password="testpass123"
        )
        self.client.login(username="insights@test.com", password="testpass123")

        self.usd = Currency.objects.create(
            code="USD", name="US Dollar", decimal_places=2, prefix="$ ", suffix=""
        )
        self.eur = Currency.objects.create(
            code="EUR", name="Euro", decimal_places=2, prefix="EUR ", suffix=""
        )
        self.group = AccountGroup.objects.create(name="Main", owner=self.user)
        self.account_a = Account.objects.create(
            name="Checking",
            group=self.group,
            currency=self.usd,
            is_archived=False,
            owner=self.user,
        )
        self.account_b = Account.objects.create(
            name="Savings",
            group=self.group,
            currency=self.eur,
            is_archived=False,
            owner=self.user,
        )

        self.category = TransactionCategory.objects.create(name="Food", owner=self.user)
        self.tag = TransactionTag.objects.create(name="Groceries", owner=self.user)
        self.entity = TransactionEntity.objects.create(name="Store", owner=self.user)

        tx1 = Transaction.objects.create(
            account=self.account_a,
            type=Transaction.Type.EXPENSE,
            is_paid=True,
            date=date(2024, 12, 15),
            reference_date=date(2024, 12, 1),
            amount=Decimal("100.00"),
            category=self.category,
            owner=self.user,
        )
        tx1.tags.add(self.tag)
        tx1.entities.add(self.entity)

        tx2 = Transaction.objects.create(
            account=self.account_a,
            type=Transaction.Type.INCOME,
            is_paid=True,
            date=date(2025, 1, 15),
            reference_date=date(2025, 1, 1),
            amount=Decimal("450.00"),
            category=self.category,
            owner=self.user,
        )
        tx2.tags.add(self.tag)
        tx2.entities.add(self.entity)

        tx3 = Transaction.objects.create(
            account=self.account_b,
            type=Transaction.Type.EXPENSE,
            is_paid=True,
            date=date(2025, 2, 10),
            reference_date=date(2025, 2, 1),
            amount=Decimal("200.00"),
            category=self.category,
            owner=self.user,
        )
        tx3.tags.add(self.tag)
        tx3.entities.add(self.entity)

    def _htmx_get(self, url):
        return self.client.get(url, HTTP_HX_REQUEST="true")

    @staticmethod
    def _dataset_value_by_code(chart_data, code, label):
        if label not in chart_data["labels"]:
            return None
        label_index = chart_data["labels"].index(label)
        for dataset in chart_data["datasets"]:
            if dataset["currency"]["code"] == code:
                return dataset["data"][label_index]
        return None

    def test_year_by_year_returns_200_for_htmx(self):
        response = self._htmx_get("/insights/year-by-year/")
        self.assertEqual(response.status_code, 200)

    def test_year_by_year_account_filter_changes_totals(self):
        all_response = self._htmx_get("/insights/year-by-year/")
        account_response = self._htmx_get(
            f"/insights/year-by-year/?account={self.account_a.id}"
        )

        all_data = all_response.context["data"]
        filtered_data = account_response.context["data"]

        self.assertEqual(
            self._dataset_value_by_code(all_data["chart"], "EUR", "2025"), -200.0
        )
        self.assertIsNone(
            self._dataset_value_by_code(filtered_data["chart"], "EUR", "2025")
        )
        self.assertEqual(
            self._dataset_value_by_code(filtered_data["chart"], "USD", "2025"), 450.0
        )

    def test_year_by_year_invalid_params_fallback(self):
        response = self._htmx_get(
            "/insights/year-by-year/?group_by=invalid&view_type=invalid&account=999999"
        )
        self.assertEqual(response.context["group_by"], "categories")
        self.assertEqual(response.context["view_type"], "table")
        self.assertEqual(response.context["selected_account"], "")

    def test_year_by_year_persists_group_view_and_account(self):
        self._htmx_get(
            f"/insights/year-by-year/?group_by=tags&view_type=graph&account={self.account_a.id}"
        )
        response = self._htmx_get("/insights/year-by-year/")

        self.assertEqual(response.context["group_by"], "tags")
        self.assertEqual(response.context["view_type"], "graph")
        self.assertEqual(response.context["selected_account"], str(self.account_a.id))

    def test_year_by_year_chart_labels_align_with_totals(self):
        response = self._htmx_get("/insights/year-by-year/")
        data = response.context["data"]

        self.assertEqual(data["chart"]["labels"], [str(year) for year in data["years"]])
        self.assertTrue(data["chart"]["datasets"])

        for year in data["years"]:
            expected = {
                currency_data["currency"]["code"]: float(currency_data["final_total"])
                for currency_data in data["year_totals"].get(year, {}).get(
                    "currencies", {}
                ).values()
            }
            for code, amount in expected.items():
                self.assertEqual(
                    self._dataset_value_by_code(data["chart"], code, str(year)), amount
                )

    def test_month_by_month_returns_200_for_htmx(self):
        response = self._htmx_get("/insights/month-by-month/")
        self.assertEqual(response.status_code, 200)

    def test_month_by_month_uses_continuous_periods_across_years(self):
        response = self._htmx_get("/insights/month-by-month/")
        periods = response.context["data"]["periods"]
        period_keys = [period["key"] for period in periods]

        self.assertEqual(period_keys, ["2024-12", "2025-01", "2025-02"])

    def test_month_by_month_has_no_year_session_dependency(self):
        session = self.client.session
        session["insights_month_by_month_year"] = 2030
        session.save()

        response = self._htmx_get("/insights/month-by-month/")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("year", response.context)

    def test_month_by_month_only_includes_months_with_data(self):
        response = self._htmx_get(f"/insights/month-by-month/?account={self.account_b.id}")
        period_keys = [period["key"] for period in response.context["data"]["periods"]]
        self.assertEqual(period_keys, ["2025-02"])

    def test_month_by_month_account_filter_correctness(self):
        all_response = self._htmx_get("/insights/month-by-month/")
        account_response = self._htmx_get(
            f"/insights/month-by-month/?account={self.account_a.id}"
        )

        all_data = all_response.context["data"]
        filtered_data = account_response.context["data"]
        all_period_labels = {
            period["key"]: period["label"] for period in all_data["periods"]
        }
        filtered_period_labels = {
            period["key"]: period["label"] for period in filtered_data["periods"]
        }

        self.assertEqual(
            self._dataset_value_by_code(
                all_data["chart"], "EUR", all_period_labels["2025-02"]
            ),
            -200.0,
        )
        self.assertIsNone(
            self._dataset_value_by_code(
                filtered_data["chart"],
                "EUR",
                filtered_period_labels.get("2025-02", "missing"),
            )
        )
        self.assertEqual(
            self._dataset_value_by_code(
                filtered_data["chart"], "USD", filtered_period_labels["2025-01"]
            ),
            450.0,
        )

    def test_month_by_month_invalid_params_fallback(self):
        response = self._htmx_get(
            "/insights/month-by-month/?group_by=invalid&view_type=invalid&account=999999"
        )
        self.assertEqual(response.context["group_by"], "categories")
        self.assertEqual(response.context["view_type"], "table")
        self.assertEqual(response.context["selected_account"], "")

    def test_month_by_month_persists_group_view_and_account(self):
        self._htmx_get(
            f"/insights/month-by-month/?group_by=entities&view_type=graph&account={self.account_b.id}"
        )
        response = self._htmx_get("/insights/month-by-month/")

        self.assertEqual(response.context["group_by"], "entities")
        self.assertEqual(response.context["view_type"], "graph")
        self.assertEqual(response.context["selected_account"], str(self.account_b.id))

    def test_month_by_month_chart_labels_align_with_totals(self):
        response = self._htmx_get("/insights/month-by-month/")
        data = response.context["data"]

        self.assertEqual(
            data["chart"]["labels"], [period["label"] for period in data["periods"]]
        )
        self.assertTrue(data["chart"]["datasets"])

        for period in data["periods"]:
            expected = {
                currency_data["currency"]["code"]: float(currency_data["final_total"])
                for currency_data in data["period_totals"]
                .get(period["key"], {})
                .get("currencies", {})
                .values()
            }
            for code, amount in expected.items():
                self.assertEqual(
                    self._dataset_value_by_code(data["chart"], code, period["label"]),
                    amount,
                )
