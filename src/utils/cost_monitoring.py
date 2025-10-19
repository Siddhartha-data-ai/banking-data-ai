"""
Databricks cost monitoring and optimization

Track and optimize:
- Cluster costs
- Query costs
- Storage costs
- DBU usage
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, window

class CostMonitor:
    """Monitor and track Databricks costs"""
    
    def __init__(self, spark: SparkSession, catalog: str = "banking_catalog"):
        self.spark = spark
        self.catalog = catalog
        self.cost_schema = f"{catalog}.cost_monitoring"
        self._init_cost_tables()
    
    def _init_cost_tables(self):
        """Initialize cost monitoring tables"""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cost_schema}")
        
        # Cluster usage table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.cost_schema}.cluster_usage (
                cluster_id STRING,
                cluster_name STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_hours DOUBLE,
                node_type STRING,
                num_workers INT,
                dbu_cost DECIMAL(18, 2),
                compute_cost DECIMAL(18, 2),
                total_cost DECIMAL(18, 2),
                user_email STRING,
                usage_date DATE
            ) USING DELTA
            PARTITIONED BY (usage_date)
        """)
        
        # Query cost table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.cost_schema}.query_costs (
                query_id STRING,
                query_text STRING,
                execution_time_ms BIGINT,
                bytes_scanned BIGINT,
                rows_scanned BIGINT,
                estimated_cost DECIMAL(18, 2),
                warehouse_id STRING,
                user_email STRING,
                query_date DATE,
                query_timestamp TIMESTAMP
            ) USING DELTA
            PARTITIONED BY (query_date)
        """)
        
        # Storage cost table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.cost_schema}.storage_costs (
                table_name STRING,
                size_gb DOUBLE,
                monthly_cost DECIMAL(18, 2),
                last_accessed TIMESTAMP,
                retention_days INT,
                optimization_recommendation STRING,
                measurement_date DATE
            ) USING DELTA
            PARTITIONED BY (measurement_date)
        """)
    
    def track_cluster_usage(
        self,
        cluster_id: str,
        cluster_name: str,
        duration_hours: float,
        node_type: str,
        num_workers: int,
        user_email: str
    ) -> Dict[str, Any]:
        """Track cluster usage and calculate costs"""
        # DBU rates (example - adjust based on actual pricing)
        dbu_rates = {
            "i3.xlarge": 0.75,      # DBU per hour
            "i3.2xlarge": 1.50,
            "m5d.large": 0.50,
            "m5d.xlarge": 1.00
        }
        
        # Compute costs (example)
        compute_rates = {
            "i3.xlarge": 0.312,     # USD per hour
            "i3.2xlarge": 0.624,
            "m5d.large": 0.096,
            "m5d.xlarge": 0.192
        }
        
        dbu_rate = dbu_rates.get(node_type, 1.0)
        compute_rate = compute_rates.get(node_type, 0.2)
        
        # Calculate costs
        dbu_cost = duration_hours * dbu_rate * (num_workers + 1)  # +1 for driver
        compute_cost = duration_hours * compute_rate * (num_workers + 1)
        total_cost = dbu_cost + compute_cost
        
        # Insert into tracking table
        cost_data = {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "start_time": datetime.now() - timedelta(hours=duration_hours),
            "end_time": datetime.now(),
            "duration_hours": duration_hours,
            "node_type": node_type,
            "num_workers": num_workers,
            "dbu_cost": float(dbu_cost),
            "compute_cost": float(compute_cost),
            "total_cost": float(total_cost),
            "user_email": user_email,
            "usage_date": datetime.now().date()
        }
        
        return cost_data
    
    def get_cost_summary(self, days: int = 30) -> Dict[str, Any]:
        """Get cost summary for last N days"""
        query = f"""
            SELECT 
                SUM(total_cost) as total_cost,
                SUM(dbu_cost) as total_dbu_cost,
                SUM(compute_cost) as total_compute_cost,
                AVG(duration_hours) as avg_duration_hours,
                COUNT(DISTINCT cluster_id) as unique_clusters,
                COUNT(DISTINCT user_email) as unique_users
            FROM {self.cost_schema}.cluster_usage
            WHERE usage_date >= current_date() - INTERVAL {days} DAYS
        """
        
        result = self.spark.sql(query).first()
        
        return {
            "period_days": days,
            "total_cost_usd": float(result["total_cost"] or 0),
            "dbu_cost_usd": float(result["total_dbu_cost"] or 0),
            "compute_cost_usd": float(result["total_compute_cost"] or 0),
            "avg_cluster_hours": float(result["avg_duration_hours"] or 0),
            "unique_clusters": int(result["unique_clusters"] or 0),
            "unique_users": int(result["unique_users"] or 0)
        }
    
    def get_top_cost_users(self, days: int = 30, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top users by cost"""
        query = f"""
            SELECT 
                user_email,
                SUM(total_cost) as total_cost,
                COUNT(DISTINCT cluster_id) as clusters_used,
                SUM(duration_hours) as total_hours
            FROM {self.cost_schema}.cluster_usage
            WHERE usage_date >= current_date() - INTERVAL {days} DAYS
            GROUP BY user_email
            ORDER BY total_cost DESC
            LIMIT {limit}
        """
        
        results = self.spark.sql(query).collect()
        
        return [
            {
                "user_email": row["user_email"],
                "total_cost_usd": float(row["total_cost"]),
                "clusters_used": int(row["clusters_used"]),
                "total_hours": float(row["total_hours"])
            }
            for row in results
        ]
    
    def optimize_recommendations(self) -> List[str]:
        """Generate cost optimization recommendations"""
        recommendations = []
        
        # Check for idle clusters
        idle_query = f"""
            SELECT COUNT(*) as idle_count
            FROM {self.cost_schema}.cluster_usage
            WHERE usage_date >= current_date() - INTERVAL 7 DAYS
              AND duration_hours > 8
        """
        
        idle_count = self.spark.sql(idle_query).first()["idle_count"]
        
        if idle_count > 10:
            recommendations.append(
                f"⚠️ Found {idle_count} long-running clusters (>8 hours). "
                "Consider auto-termination policies."
            )
        
        # Check for oversized clusters
        recommendations.append(
            "💡 Review cluster sizes - consider right-sizing based on actual workload."
        )
        
        # Storage recommendations
        recommendations.append(
            "💡 Run OPTIMIZE and VACUUM on large Delta tables to reduce storage costs."
        )
        
        return recommendations

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CostMonitoring").getOrCreate()
    
    monitor = CostMonitor(spark)
    
    # Track cluster usage
    cost_data = monitor.track_cluster_usage(
        cluster_id="cluster-123",
        cluster_name="ETL Cluster",
        duration_hours=2.5,
        node_type="i3.xlarge",
        num_workers=4,
        user_email="analyst@bank.com"
    )
    
    print(f"Cluster cost: ${cost_data['total_cost']:.2f}")
    
    # Get cost summary
    summary = monitor.get_cost_summary(days=30)
    print(f"30-day cost: ${summary['total_cost_usd']:.2f}")
    
    # Get recommendations
    recommendations = monitor.optimize_recommendations()
    for rec in recommendations:
        print(rec)

