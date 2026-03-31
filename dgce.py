"""Thin CLI shim for `python -m dgce`."""

from aether.dgce.inspector import main


if __name__ == "__main__":
    raise SystemExit(main())
