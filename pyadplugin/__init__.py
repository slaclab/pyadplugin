from .server import ADPluginServer, ADPluginFunction # NOQA

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
