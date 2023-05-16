import pickle
import hashlib


def generate_hash(template: str, args: dict) -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    """
    t = tuple(sorted(args.items(), key=lambda x: x[0]))
    obj = (template, t)
    return hashlib.md5(pickle.dumps(obj)).hexdigest()