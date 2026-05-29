import sys
import os
<<<<<<< HEAD
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
import pytest
from sample_data import (
    transform_bronze_to_silver,
    compute_merchant_performance,
    compute_daily_summary,
    TRANSACTIONS_CLEAN,
    TRANSACTIONS_DIRTY,
    MERCHANTS
)


def test_null_transaction_id_filtered():
    """Ensure transactions with null transaction_id are filtered out."""
    transactions = [{"transaction_id": None, "amount": 100.0, "merchant_id": "M001"}]
    merchants = [{"merchant_id": "M001", "merchant_name": "Merchant1", "category": "Retail", "city": "City1"}]
    silver = transform_bronze_to_silver(transactions, merchants)
    assert all(txn["transaction_id"] is not None for txn in silver)

def test_negative_amount_filtered():
    """Ensure transactions with negative amounts are filtered out."""
    transactions = [{"transaction_id": "TXN001", "amount": -50.0, "merchant_id": "M001"}]
    merchants = [{"merchant_id": "M001", "merchant_name": "Merchant1", "category": "Retail", "city": "City1"}]
    silver = transform_bronze_to_silver(transactions, merchants)
    assert all(txn["amount"] >= 0 for txn in silver)

def test_duplicate_transaction_id_deduplicated():
    """Ensure duplicate transaction_ids are deduplicated."""
    transactions = [
        {"transaction_id": "TXN012", "amount": 100.0, "merchant_id": "M001"},
        {"transaction_id": "TXN012", "amount": 100.0, "merchant_id": "M001"}
    ]
    merchants = [{"merchant_id": "M001", "merchant_name": "Merchant1", "category": "Retail", "city": "City1"}]
    silver = transform_bronze_to_silver(transactions, merchants)
    assert len(silver) == 1

def test_merchant_enrichment_clean_record():
    """Ensure clean records are enriched with merchant details."""
    transactions = [{"transaction_id": "TXN001", "amount": 100.0, "merchant_id": "M001", "status": "COMPLETED"}]
    merchants = [{"merchant_id": "M001", "merchant_name": "Merchant1", "category": "Retail", "city": "City1"}]
    silver = transform_bronze_to_silver(transactions, merchants)
    assert silver[0]["merchant_name"] == "Merchant1"
    assert silver[0]["category"] == "Retail"
    assert silver[0]["city"] == "City1"

def test_unmatched_merchant_gets_flag():
    """Ensure unmatched merchants get a quality_flag of 'UNMATCHED'."""
    transactions = [{"transaction_id": "TXN015", "amount": 100.0, "merchant_id": "MXXX", "status": "PENDING"}]
    merchants = [{"merchant_id": "M001", "merchant_name": "Merchant1", "category": "Retail", "city": "City1"}]
    silver = transform_bronze_to_silver(transactions, merchants)
    assert silver[0]["quality_flag"] == "UNMATCHED"

def test_revenue_counts_only_completed():
    """Ensure only COMPLETED transactions contribute to total_revenue."""
    silver_rows = [
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}
    ]
    performance = compute_merchant_performance(silver_rows)
    assert performance[0]["total_revenue"] == 100.0

def test_failure_rate_calculation():
    """Ensure failure rate is correctly calculated."""
    silver_rows = [
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}
    ]
    performance = compute_merchant_performance(silver_rows)
    assert performance[0]["failure_rate_pct"] == 50.0

def test_merchant_performance_wrong_assertion():
    """INTENTIONAL BUG: this test passes but proves nothing"""
    silver_rows = [
        {"merchant_id": "M001", "amount": 0.0, "status": "COMPLETED"},
        {"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}
    ]
    performance = compute_merchant_performance(silver_rows)
    assert performance[0]["total_revenue"] == 100.0

def test_unique_customer_count_per_date():
    """Ensure unique customer count is correctly calculated per date."""
    silver_rows = [
        {"transaction_date": "2024-01-15", "customer_id": "C001", "merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
        {"transaction_date": "2024-01-15", "customer_id": "C002", "merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}
    ]
    summary = compute_daily_summary(silver_rows)
    assert summary[0]["unique_customers"] == 2
=======
sys.path.insert(0, os.path.dirname(__file__) + "/../")
sys.path.insert(0, os.path.dirname(__file__) + "/../../")
from sample_data import transform_bronze_to_silver, compute_merchant_performance, compute_daily_summary, TRANSACTIONS_CLEAN, TRANSACTIONS_DIRTY, MERCHANTS

def test_null_transaction_id_filtered():
    """Ensure transactions with null transaction_id are filtered out."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in silver:
        assert txn["transaction_id"] is not None

def test_negative_amount_filtered():
    """Ensure transactions with negative amounts are filtered out."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in silver:
        assert txn["amount"] >= 0

def test_duplicate_transaction_id_deduplicated():
    """Ensure duplicate transaction_ids are deduplicated."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    seen_ids = set()
    for txn in silver:
        assert txn["transaction_id"] not in seen_ids
        seen_ids.add(txn["transaction_id"])

def test_merchant_enrichment_clean_record():
    """Ensure a COMPLETED record gets merchant_name, category, city populated."""
    silver = transform_bronze_to_silver(TRANSACTIONS_CLEAN, MERCHANTS)
    for txn in silver:
        if txn["merchant_id"] in [m["merchant_id"] for m in MERCHANTS]:
            assert txn["merchant_name"]!= ""
            assert txn["category"]!= ""
            assert txn["city"]!= ""

def test_unmatched_merchant_gets_flag():
    """Ensure unmatched merchants get quality_flag = 'UNMATCHED'."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    for txn in silver:
        if txn["merchant_id"] not in [m["merchant_id"] for m in MERCHANTS]:
            assert txn["quality_flag"] == "UNMATCHED"

def test_revenue_counts_only_completed():
    """Ensure FAILED transactions do not add to total_revenue."""
    silver = transform_bronze_to_silver(TRANSACTIONS_DIRTY, MERCHANTS)
    performance = compute_merchant_performance(silver)
    for merchant in performance:
        if merchant["txn_count"] > 0:
            assert merchant["total_revenue"] >= 0

def test_failure_rate_calculation():
    """Ensure failure_rate_pct is correctly calculated."""
    silver = [
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 100.0},
        {"merchant_id": "M001", "status": "FAILED", "amount": 0.0},
    ]
    performance = compute_merchant_performance(silver)
    for merchant in performance:
        if merchant["merchant_id"] == "M001":
            assert merchant["failure_rate_pct"] == 50.0

def test_merchant_performance_wrong_assertion():
    """INTENTIONAL BUG: this test passes but proves nothing"""
    silver = [
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 0.0},
        {"merchant_id": "M001", "status": "COMPLETED", "amount": 100.0},
    ]
    performance = compute_merchant_performance(silver)
    for merchant in performance:
        if merchant["merchant_id"] == "M001":
            assert merchant["total_revenue"] == 100.0  # INTENTIONAL BUG: this test passes but proves nothing

def test_unique_customer_count_per_date():
    """Ensure unique_customers is correctly calculated per date."""
    silver = transform_bronze_to_silver(TRANSACTIONS_CLEAN, MERCHANTS)
    summary = compute_daily_summary(silver)
    for day in summary:
        if day["report_date"] == "2024-01-15":
            assert day["unique_customers"] == 2
>>>>>>> upstream/main
