"""
Launch script for GDPR Right to be Forgotten Dashboard
Run this in Databricks or local environment with Databricks connection
"""

import os
import subprocess

def main():
    """Launch the GDPR dashboard"""
    
    print("🚀 Launching GDPR Right to be Forgotten Dashboard...")
    print("=" * 60)
    
    # Get the dashboard script path
    dashboard_path = os.path.join(
        os.path.dirname(__file__),
        "gdpr_right_to_be_forgotten_dashboard.py"
    )
    
    # Launch with streamlit
    try:
        subprocess.run([
            "streamlit", "run", dashboard_path,
            "--server.port", "8502",
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
    ║   🗑️  GDPR RIGHT TO BE FORGOTTEN DASHBOARD              ║
    ║                                                           ║
    ║   Automated Customer Data Erasure Management             ║
    ║   GDPR Article 17 Compliance                            ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    main()

