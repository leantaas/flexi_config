__version__ = '1.0.4'
try:
    from .config import Config
except ImportError:
    # skipping import during setup
    pass
