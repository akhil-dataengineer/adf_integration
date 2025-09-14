import os
import subprocess


# ---------------------------
# GitHub commit wrapper
# ---------------------------
def git_commit(branch: str, files: list, commit_message: str = "AIDE Agent auto-commit"):
    """
    Commit and push files to a given GitHub branch.
    Args:
        branch (str): The Git branch to push to (e.g., 'main', 'dev').
        files (list): List of file paths to add/commit.
        commit_message (str): Commit message.
    """
    try:
        # Ensure we are on the right branch
        subprocess.run(["git", "checkout", branch], check=True)

        # Add files
        subprocess.run(["git", "add"] + files, check=True)

        # Commit changes
        subprocess.run(["git", "commit", "-m", commit_message], check=True)

        # Push to remote
        subprocess.run(["git", "push", "origin", branch], check=True)

        print(f"✅ Successfully committed and pushed {files} to branch '{branch}'.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git operation failed: {e}")


# ---------------------------
# Test locally
# ---------------------------
if __name__ == "__main__":
    # Example: commit a new pipeline JSON to repo
    test_files = ["pipelines/agent_test_pipeline.json"]  # replace with your actual path
    git_commit("main", test_files, "Add agent_test_pipeline via AIDE Agent")
