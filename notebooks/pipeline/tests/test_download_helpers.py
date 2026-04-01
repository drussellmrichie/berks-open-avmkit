"""
Tests for download_berks_parcels helpers: _safe_float, _extract_sales,
and the portfolio/bulk-sale flagging logic.
"""

import pandas as pd
import pytest

from download_berks_parcels import _safe_float, _extract_sales, SALES_MIN_YEAR


# ---------------------------------------------------------------------------
# _safe_float
# ---------------------------------------------------------------------------

class TestSafeFloat:
    def test_valid_numeric_string(self):
        assert _safe_float("123.4") == 123.4

    def test_valid_integer(self):
        assert _safe_float(5) == 5.0

    def test_valid_float(self):
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_nan_returns_none(self):
        assert _safe_float(float("nan")) is None

    def test_non_numeric_string_returns_none(self):
        assert _safe_float("abc") is None

    def test_empty_string_returns_none(self):
        assert _safe_float("") is None

    def test_zero_returns_zero(self):
        assert _safe_float(0) == 0.0

    def test_negative_value(self):
        assert _safe_float(-100.5) == pytest.approx(-100.5)

    def test_whitespace_string_returns_none(self):
        assert _safe_float("  ") is None


# ---------------------------------------------------------------------------
# _extract_sales
# ---------------------------------------------------------------------------

def _cama_row(parid="12345", price=150_000, saledt=1_609_459_200_000,
              yr1=None, mth1=None, pr1=None,
              yr2=None, mth2=None, pr2=None):
    """Build a minimal CAMA row for _extract_sales testing.
    saledt default = 2021-01-01 00:00:00 UTC in milliseconds.
    """
    row = {
        "PARID": parid,
        "PRICE": price,
        "SALEDT": saledt,
    }
    for i, (yr, mth, pr) in enumerate([(yr1, mth1, pr1), (yr2, mth2, pr2)], start=1):
        row[f"SALEYR{i}"]  = str(yr) if yr is not None else None
        row[f"SALEMTH{i}"] = str(mth).zfill(2) if mth is not None else None
        row[f"SALEPR{i}"]  = pr
    # Unused slot 3
    row["SALEYR3"] = None
    row["SALEMTH3"] = None
    row["SALEPR3"] = None
    return row


class TestExtractSales:
    def test_most_recent_sale_from_ms_epoch(self):
        df = pd.DataFrame([_cama_row()])
        result = _extract_sales(df)
        assert len(result) >= 1
        assert result.iloc[0]["key"] == "12345"
        assert result.iloc[0]["sale_price"] == 150_000

    def test_empty_parid_skipped(self):
        df = pd.DataFrame([_cama_row(parid="")])
        result = _extract_sales(df)
        assert len(result) == 0

    def test_null_parid_skipped(self):
        df = pd.DataFrame([_cama_row(parid=None)])
        result = _extract_sales(df)
        assert len(result) == 0

    def test_sale_before_min_year_excluded(self):
        # 2010-01-01 UTC in ms
        old_ms = int(pd.Timestamp("2010-01-01", tz="UTC").timestamp() * 1000)
        df = pd.DataFrame([_cama_row(saledt=old_ms)])
        result = _extract_sales(df)
        assert len(result) == 0

    def test_historical_saleyr_salemth_fields_parsed(self):
        df = pd.DataFrame([_cama_row(price=None, saledt=None,
                                     yr1=2022, mth1=6, pr1=200_000)])
        result = _extract_sales(df)
        assert any(result["sale_price"] == 200_000)
        assert any(result["sale_date"] == pd.Timestamp("2022-06-01"))

    def test_historical_sale_before_min_year_excluded(self):
        df = pd.DataFrame([_cama_row(price=None, saledt=None,
                                     yr1=SALES_MIN_YEAR - 1, mth1=1, pr1=100_000)])
        result = _extract_sales(df)
        assert len(result) == 0

    def test_arms_length_heuristic_valid_above_10k(self):
        df = pd.DataFrame([_cama_row(parid="A", price=50_000)])
        result = _extract_sales(df)
        assert result[result["key"] == "A"]["valid_sale"].all()

    def test_arms_length_heuristic_invalid_below_10k(self):
        df = pd.DataFrame([_cama_row(parid="B", price=5_000)])
        result = _extract_sales(df)
        assert not result[result["key"] == "B"]["valid_sale"].any()

    def test_arms_length_heuristic_boundary_exactly_10k(self):
        df = pd.DataFrame([_cama_row(price=10_000)])
        result = _extract_sales(df)
        assert result.iloc[0]["valid_sale"] is True or result.iloc[0]["valid_sale"] == True

    def test_dedup_on_key_sale_no_duplicates(self):
        row = _cama_row()
        df = pd.DataFrame([row, row])  # exact duplicate rows
        result = _extract_sales(df)
        assert result["key_sale"].duplicated().sum() == 0

    def test_dedup_on_key_date_price(self):
        # Same parcel, same price — once from PRICE/SALEDT, once from SALEYR1/SALEMTH1
        # They map to the same date (2021-01-01) so should dedup.
        row = _cama_row(price=100_000, saledt=1_609_459_200_000,
                        yr1=2021, mth1=1, pr1=100_000)
        df = pd.DataFrame([row])
        result = _extract_sales(df)
        dupes = result.duplicated(subset=["key", "sale_date", "sale_price"]).sum()
        assert dupes == 0

    def test_empty_cama_returns_empty_dataframe(self):
        df = pd.DataFrame(columns=["PARID", "PRICE", "SALEDT",
                                   "SALEYR1", "SALEMTH1", "SALEPR1",
                                   "SALEYR2", "SALEMTH2", "SALEPR2",
                                   "SALEYR3", "SALEMTH3", "SALEPR3"])
        result = _extract_sales(df)
        assert len(result) == 0
        for col in ["key_sale", "key", "sale_date", "sale_price", "valid_sale", "vacant_sale"]:
            assert col in result.columns

    def test_vacant_sale_initialised_false(self):
        df = pd.DataFrame([_cama_row()])
        result = _extract_sales(df)
        assert (result["vacant_sale"] == False).all()

    def test_multiple_historical_slots_extracted(self):
        row = _cama_row(price=None, saledt=None,
                        yr1=2021, mth1=3, pr1=120_000,
                        yr2=2022, mth2=9, pr2=180_000)
        df = pd.DataFrame([row])
        result = _extract_sales(df)
        prices = set(result["sale_price"])
        assert 120_000 in prices
        assert 180_000 in prices


# ---------------------------------------------------------------------------
# Portfolio / bulk-sale flagging (logic mirrored from download_berks_parcels.main)
# ---------------------------------------------------------------------------

def _apply_portfolio_flag(sales: pd.DataFrame) -> pd.DataFrame:
    """Replicate the portfolio-flagging logic from download_berks_parcels.main."""
    portfolio_mask = (
        sales.groupby(["sale_date", "sale_price"])["key"].transform("count") >= 5
    )
    sales = sales.copy()
    sales.loc[portfolio_mask, "valid_sale"] = False
    return sales


class TestPortfolioFlagging:
    def _sales(self, n, price=100_000.0, date="2022-01-01"):
        return pd.DataFrame({
            "key":        [f"P{i}" for i in range(n)],
            "sale_date":  [pd.Timestamp(date)] * n,
            "sale_price": [price] * n,
            "valid_sale": [True] * n,
            "vacant_sale":[False] * n,
        })

    def test_four_same_price_date_remain_valid(self):
        sales = self._sales(4)
        result = _apply_portfolio_flag(sales)
        assert result["valid_sale"].all()

    def test_five_same_price_date_flagged_invalid(self):
        sales = self._sales(5)
        result = _apply_portfolio_flag(sales)
        assert not result["valid_sale"].any()

    def test_six_same_price_date_flagged_invalid(self):
        sales = self._sales(6)
        result = _apply_portfolio_flag(sales)
        assert not result["valid_sale"].any()

    def test_portfolio_group_does_not_affect_other_groups(self):
        """5 matching rows should only invalidate those rows, not unrelated ones."""
        portfolio = self._sales(5, price=100_000.0, date="2022-01-01")
        normal    = self._sales(3, price=200_000.0, date="2022-02-01")
        # Give unique keys to avoid collision
        normal["key"] = [f"Q{i}" for i in range(3)]
        sales = pd.concat([portfolio, normal], ignore_index=True)
        result = _apply_portfolio_flag(sales)
        assert not result.iloc[:5]["valid_sale"].any()
        assert result.iloc[5:]["valid_sale"].all()

    def test_threshold_is_count_per_group_not_global(self):
        """Two groups of 4 (different prices) should each stay valid."""
        g1 = self._sales(4, price=100_000.0, date="2022-01-01")
        g2 = self._sales(4, price=200_000.0, date="2022-01-01")
        g2["key"] = [f"Q{i}" for i in range(4)]
        sales = pd.concat([g1, g2], ignore_index=True)
        result = _apply_portfolio_flag(sales)
        assert result["valid_sale"].all()
