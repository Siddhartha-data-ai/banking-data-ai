"""
Launch script for Banking AI Chatbot

This script sets up and launches the Streamlit chatbot application.
"""

import os
import sys
import subprocess

def main():
    """Launch the banking chatbot"""
    
    print("=" * 60)
    print("Banking AI Chatbot Launcher")
    print("=" * 60)
    
    # Check for required environment variables
    required_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("\n⚠️  Warning: Missing environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease set these environment variables before running the chatbot.")
        print("\nExample:")
        print("  export DATABRICKS_HOST='your-workspace.cloud.databricks.com'")
        print("  export DATABRICKS_TOKEN='your-token'")
        print("  export DATABRICKS_HTTP_PATH='/sql/1.0/warehouses/your-warehouse-id'")
        print("\nOr create a .env file with these values.")
        
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    chatbot_path = os.path.join(script_dir, "banking_chatbot.py")
    
    print("\n✓ Launching Banking AI Chatbot...")
    print(f"✓ Running: streamlit run {chatbot_path}")
    print("\nThe chatbot will open in your browser at http://localhost:8501")
    print("\nPress Ctrl+C to stop the chatbot\n")
    
    # Launch Streamlit
    try:
        subprocess.run([
            "streamlit", "run", chatbot_path,
            "--server.port", "8501",
            "--server.headless", "true"
        ], check=True)
    except KeyboardInterrupt:
        print("\n\nChatbot stopped by user")
    except FileNotFoundError:
        print("\n✗ Error: Streamlit not found!")
        print("Install it with: pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error launching chatbot: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

