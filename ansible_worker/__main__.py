"""Entry point for running as python -m ansible_worker."""

import sys

from ansible_worker.main import main

if __name__ == "__main__":
    sys.exit(main())
