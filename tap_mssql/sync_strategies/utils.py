
import datetime
import secrets
import string






def generate_random_string(length: int = 8) -> str:
    """
    Generate cryptographically secure random strings
    Uses best practice from Python doc https://docs.python.org/3/library/secrets.html#recipes-and-best-practices
    Args:
        length: length of the string to generate
    Returns: random string
    """

    if length < 1:
        raise Exception('Length must be at least 1!')

    if 0 < length < 8:
        warnings.warn('Length is too small! consider 8 or more characters')

    return ''.join(
        secrets.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def gen_export_filename(
    tap_id: str, table: str, suffix: str = None, postfix: str = None, ext: str = None
) -> str:
    """
    Generates a unique filename used for exported fastsync data that avoids file name collision
    Default pattern:
        pipelinewise_<tap_id>_<table>_<timestamp_with_ms>_fastsync_<random_string>.csv.gz
    Args:
        tap_id: Unique tap id
        table: Name of the table to export
        suffix: Generated filename suffix. Defaults to current timestamp in milliseconds
        postfix: Generated filename postfix. Defaults to a random 8 character length string
        ext: Filename extension. Defaults to .csv.gz
    Returns:
        Unique filename as a string
    """
    if not suffix:
        suffix = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')

    if not postfix:
        postfix = generate_random_string()

    if not ext:
        ext = 'csv.gz'

    return 'pipelinewise_{}_{}_{}_fastsync_{}.{}'.format(
        tap_id, table, suffix, postfix, ext
    )
