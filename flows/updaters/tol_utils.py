"""Shared utilities for ToL portal updates."""


def connect_to_portal(filter_spec: dict):
    """
    Connect to the ToL portal and retrieve a filtered dataset.

    Args:
        filter_spec (dict): DataSourceFilter specification as dict.
            Example: {"gn_date_published": {"exists": {}}}

    Returns:
        list: Filtered list of portal objects.

    Raises:
        RuntimeError: If portal connection fails.
    """
    # Import here to defer tol dependency and avoid Pydantic conflicts at import time
    from tol.core import DataSourceFilter
    from tol.sources.portal import portal

    print("Connecting to ToL portal...")
    try:
        prtl = portal()
        data_filter = DataSourceFilter()
        data_filter.and_ = filter_spec
        return prtl.get_list("species", object_filters=data_filter)
    except Exception as e:
        raise RuntimeError(f"Failed to connect to ToL portal: {e}") from e


def get_field_value(obj, field_spec, field_name=None):
    """
    Get value from object using string path or callable.

    Handles both string paths (dot-notation) and callable functions.

    Args:
        obj: Object to extract value from.
        field_spec (str or callable): Either a dot-notation path string
            or a callable that takes (obj, field_name).
        field_name (str, optional): Field name for callables.

    Returns:
        Value extracted from object.
    """
    if not isinstance(field_spec, str):
        # It's a callable
        return field_spec(obj, field_name) if field_name else field_spec(obj)

    # It's a string path
    value = obj
    for attr in field_spec.split("."):
        value = getattr(value, attr)
        if value is None:
            return None
    return value
