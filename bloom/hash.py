try:
    from ._hashc import *  # noqa: F403
except ImportError:
    from ._hashpy import *  # noqa: F403
