from .base import Archive
from .git import GitArchive

Archive.register_type(GitArchive)
