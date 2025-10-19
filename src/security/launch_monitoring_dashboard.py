"""
Launch script for Sensitive Data Access Monitoring Dashboard
Run this in Databricks or local environment with Databricks connection
"""

import os
import subprocess

def main():
    """Launch the monitoring dashboard"""
    
    print("🚀 Launching Sensitive Data Access Monitoring Dashboard...")
    print("=" * 60)
    
    # Get the dashboard script path
    dashboard_path = os.path.join(
        os.path.dirname(__file__),
        "sensitive_data_monitoring_dashboard.py"
    )
    
    # Launch with streamlit
    try:
        subprocess.run([
            "streamlit", "run", dashboard_path,
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ])
    except KeyboardInterrupt:
        print("\n\n✅ Dashboard stopped by user")
    except Exception as e:
        print(f"\n❌ Error launching dashboard: {e}")
        print("\nPlease ensure:")
        print("  1. Streamlit is installed: pip install streamlit")
        print("  2. You have valid Databricks credentials")
        print("  3. You're connected to the Databricks workspace")

if __name__ == "__main__":
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║   🔒 SENSITIVE DATA ACCESS MONITORING DASHBOARD          ║
    ║                                                           ║
    ║   Real-time monitoring of PII and sensitive data access  ║
    ║   Banking Platform Security & Compliance                 ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    main()

