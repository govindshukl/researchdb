"""
Generate sample fraud transaction data for the ResearchDB POC.
Creates 1000 transactions with ~5% fraud rate across multiple channels and merchants.
"""

import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from faker import Faker
from src.database.connection import get_db

fake = Faker()
random.seed(42)  # For reproducibility


class SampleDataGenerator:
    """Generates realistic sample data for fraud detection research."""

    def __init__(self, db_connection):
        self.db = db_connection
        self.customers = []
        self.merchants = []
        self.channels = []

    def generate_customers(self, count: int = 200):
        """Generate customer records."""
        print(f"Generating {count} customers...")

        regions = ['GCC', 'MENA', 'Europe', 'Asia']
        segments = ['Retail', 'Corporate', 'SME', 'Private Banking']

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            for _ in range(count):
                name = fake.name()
                region = random.choice(regions)
                segment = random.choice(segments)

                cursor.execute(
                    """
                    INSERT INTO customers (name, region, segment)
                    VALUES (?, ?, ?)
                    """,
                    (name, region, segment)
                )

            conn.commit()

        # Load generated customers
        self.customers = self.db.execute_query("SELECT customer_id FROM customers")
        print(f"Created {len(self.customers)} customers")

    def generate_merchants(self, count: int = 100):
        """Generate merchant records."""
        print(f"Generating {count} merchants...")

        # Get MCC codes
        mcc_codes = self.db.execute_query("SELECT mcc_code FROM mcc_codes")
        mcc_list = [row['mcc_code'] for row in mcc_codes]

        risk_tiers = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        countries = ['US', 'UK', 'AE', 'SA', 'BH', 'IN', 'CN', 'SG']

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            for _ in range(count):
                name = fake.company()
                mcc_code = random.choice(mcc_list)
                risk_tier = random.choice(risk_tiers)
                country = random.choice(countries)

                cursor.execute(
                    """
                    INSERT INTO merchants (name, mcc_code, risk_tier, country)
                    VALUES (?, ?, ?, ?)
                    """,
                    (name, mcc_code, risk_tier, country)
                )

            conn.commit()

        # Load generated merchants
        self.merchants = self.db.execute_query("SELECT merchant_id, risk_tier FROM merchants")
        print(f"Created {len(self.merchants)} merchants")

    def generate_transactions(self, count: int = 1000, fraud_rate: float = 0.05):
        """
        Generate transaction records with fraud labels.

        Args:
            count: Number of transactions to generate
            fraud_rate: Percentage of fraudulent transactions (default 5%)
        """
        print(f"Generating {count} transactions (fraud rate: {fraud_rate*100}%)...")

        # Get channel IDs
        channels = self.db.execute_query("SELECT channel_id FROM channels")
        channel_ids = [row['channel_id'] for row in channels]

        # Generate date range: last 6 months
        end_date = datetime.now()
        start_date = end_date - timedelta(days=180)

        with self.db.get_connection() as conn:
            cursor = conn.cursor()

            for i in range(count):
                # Random selections
                customer = random.choice(self.customers)
                merchant = random.choice(self.merchants)
                channel = random.choice(channel_ids)

                # Transaction amount (log-normal distribution for realism)
                amount = round(random.lognormvariate(4, 1.5), 2)  # Mean ~$150, wide spread

                # Transaction date (weighted toward recent dates)
                days_ago = int(random.expovariate(1/30))  # Exponential: more recent transactions
                days_ago = min(days_ago, 180)  # Cap at 180 days
                txn_date = end_date - timedelta(days=days_ago)

                # Determine if fraudulent
                # Higher fraud probability for:
                # - High-risk merchants
                # - Large amounts (>$1000)
                # - Online/Mobile channels (channels 3,4)
                base_fraud_prob = fraud_rate
                if merchant['risk_tier'] in ['HIGH', 'CRITICAL']:
                    base_fraud_prob *= 3
                if amount > 1000:
                    base_fraud_prob *= 2
                if channel in [3, 4]:  # Online, Mobile
                    base_fraud_prob *= 1.5

                fraud_flag = 1 if random.random() < base_fraud_prob else 0

                # Fraud score (0-1): higher for actual fraud
                if fraud_flag == 1:
                    fraud_score = round(random.uniform(0.7, 0.99), 4)
                else:
                    fraud_score = round(random.uniform(0.0, 0.3), 4)

                # Status
                status = 'COMPLETED' if fraud_flag == 0 else random.choice(['COMPLETED', 'REVERSED'])

                cursor.execute(
                    """
                    INSERT INTO transactions (
                        customer_id, merchant_id, channel_id,
                        amount, txn_date, fraud_flag, fraud_score, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        customer['customer_id'],
                        merchant['merchant_id'],
                        channel,
                        amount,
                        txn_date.strftime('%Y-%m-%d'),
                        fraud_flag,
                        fraud_score,
                        status
                    )
                )

                if (i + 1) % 100 == 0:
                    print(f"  Generated {i + 1}/{count} transactions...")

            conn.commit()

        # Print fraud statistics
        fraud_count = self.db.execute_query(
            "SELECT COUNT(*) as count FROM transactions WHERE fraud_flag = 1"
        )[0]['count']

        print(f"Created {count} transactions")
        print(f"  Fraud transactions: {fraud_count} ({fraud_count/count*100:.1f}%)")
        print(f"  Legitimate transactions: {count - fraud_count} ({(count-fraud_count)/count*100:.1f}%)")

    def generate_all(self, num_customers: int = 200, num_merchants: int = 100, num_transactions: int = 1000):
        """
        Generate complete sample dataset.

        Args:
            num_customers: Number of customers
            num_merchants: Number of merchants
            num_transactions: Number of transactions
        """
        print("\n" + "="*60)
        print("ResearchDB Sample Data Generation")
        print("="*60 + "\n")

        self.generate_customers(num_customers)
        self.generate_merchants(num_merchants)
        self.generate_transactions(num_transactions)

        print("\n" + "="*60)
        print("Sample Data Generation Complete!")
        print("="*60 + "\n")

    def print_statistics(self):
        """Print dataset statistics."""
        print("\n" + "="*60)
        print("Dataset Statistics")
        print("="*60 + "\n")

        # Table counts
        tables = ['customers', 'merchants', 'channels', 'transactions']
        for table in tables:
            count = self.db.get_row_count(table)
            print(f"{table.capitalize():20s}: {count:>10,} rows")

        # Fraud breakdown by channel
        print("\nFraud by Channel:")
        query = """
            SELECT
                c.name AS channel_name,
                COUNT(*) AS total_txns,
                SUM(t.fraud_flag) AS fraud_txns,
                ROUND(SUM(t.fraud_flag) * 100.0 / COUNT(*), 2) AS fraud_rate
            FROM transactions t
            JOIN channels c ON t.channel_id = c.channel_id
            GROUP BY c.name
            ORDER BY fraud_rate DESC
        """
        results = self.db.execute_query(query)
        for row in results:
            print(f"  {row['channel_name']:20s}: {row['fraud_txns']:>4}/{row['total_txns']:<4} ({row['fraud_rate']:>5}%)")

        # Fraud by merchant risk tier
        print("\nFraud by Merchant Risk Tier:")
        query = """
            SELECT
                m.risk_tier,
                COUNT(*) AS total_txns,
                SUM(t.fraud_flag) AS fraud_txns,
                ROUND(SUM(t.fraud_flag) * 100.0 / COUNT(*), 2) AS fraud_rate
            FROM transactions t
            JOIN merchants m ON t.merchant_id = m.merchant_id
            GROUP BY m.risk_tier
            ORDER BY fraud_rate DESC
        """
        results = self.db.execute_query(query)
        for row in results:
            print(f"  {row['risk_tier']:20s}: {row['fraud_txns']:>4}/{row['total_txns']:<4} ({row['fraud_rate']:>5}%)")

        # Amount statistics
        print("\nTransaction Amount Statistics:")
        query = """
            SELECT
                ROUND(AVG(amount), 2) AS avg_amount,
                ROUND(MIN(amount), 2) AS min_amount,
                ROUND(MAX(amount), 2) AS max_amount,
                ROUND(SUM(amount), 2) AS total_amount
            FROM transactions
        """
        result = self.db.execute_query(query)[0]
        print(f"  Average: ${result['avg_amount']:>10,.2f}")
        print(f"  Minimum: ${result['min_amount']:>10,.2f}")
        print(f"  Maximum: ${result['max_amount']:>10,.2f}")
        print(f"  Total:   ${result['total_amount']:>10,.2f}")

        print("\n" + "="*60 + "\n")


def main():
    """Main execution function."""
    # Initialize database connection
    db_path = Path(__file__).parent.parent / "data" / "researchdb.sqlite"
    db = get_db(str(db_path))

    # Create generator
    generator = SampleDataGenerator(db)

    # Generate all data
    generator.generate_all(
        num_customers=200,
        num_merchants=100,
        num_transactions=1000
    )

    # Print statistics
    generator.print_statistics()


if __name__ == "__main__":
    main()
